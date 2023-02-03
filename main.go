package main

import (
	"database/sql"
	"embed"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/bwmarrin/discordgo"
)

var (
	guildID, channelID string

	knownMemberStateLock  sync.RWMutex
	knownMemberState      map[string]discordUser
	knownMemberStateEmpty bool

	db                              *sql.DB
	stmtAdd, stmtUpdate, stmtRemove *sql.Stmt
)

type discordUser struct {
	username      string
	discriminator string
}

func main() {
	var err error

	authenticationToken := os.Getenv("DUL_TOKEN")
	guildID = os.Getenv("DUL_GUILD_ID")
	channelID = os.Getenv("DUL_CHANNEL_ID")
	statePath := os.Getenv("DUL_STATE_PATH")
	if statePath == "" {
		statePath = "./dul.db"
	}

	if authenticationToken == "" || guildID == "" || channelID == "" {
		log.Fatal("require DUL_TOKEN, DUL_GUILD_ID, DUL_CHANNEL_ID")
	}

	db, err = sql.Open("sqlite3", statePath)
	if err != nil {
		log.Fatalf("failed to open sqlite db at %v: %v", statePath, err)
	}
	defer db.Close()

	if err := migrate(db); err != nil {
		log.Fatalf("failed to migrate: %v", err)
	}

	stmtAdd, err = db.Prepare("INSERT INTO members(discord_id, discord_username, discord_discriminator) VALUES (?, ?, ?)")
	if err != nil {
		log.Fatalf("failed to prepare INSERT statement: %v", err)
	}

	stmtUpdate, err = db.Prepare("UPDATE members SET discord_username = ?, discord_discriminator = ? WHERE discord_id = ?")
	if err != nil {
		log.Fatalf("failed to prepare UPDATE statement: %v", err)
	}

	stmtRemove, err = db.Prepare("DELETE FROM members WHERE discord_id = ?")
	if err != nil {
		log.Fatalf("failed to prepare DELETE statement: %v", err)
	}

	// load members from persistent storage
	knownMemberState = map[string]discordUser{}
	{
		rows, err := db.Query("SELECT discord_id, discord_username, discord_discriminator FROM members")
		if err != nil {
			log.Fatalf("failed to query members: %v", err)
		}

		var discordID string
		for rows.Next() {
			discordUser := discordUser{}
			if err = rows.Scan(&discordID, &discordUser.username, &discordUser.discriminator); err != nil {
				log.Fatalf("failed scanning discord ID from row: %v", err)
			}
			knownMemberState[discordID] = discordUser
		}

		rows.Close()
		loadedCount := len(knownMemberState)
		if loadedCount == 0 {
			knownMemberStateEmpty = true
			log.Println("loaded no members from DB, assuming first time load, squelching notifications")
		} else {
			knownMemberStateEmpty = false
			log.Printf("loaded %v members from DB", loadedCount)
		}
	}

	session, err := discordgo.New("Bot " + authenticationToken)
	if err != nil {
		log.Fatal("failed to create discord session: ", err)
	}
	session.AddHandler(ready)
	session.AddHandler(guildMemberAdd)
	session.AddHandler(guildMemberRemove)

	session.Identify.Intents = discordgo.IntentsGuildMembers // this is a privileged intent

	if err := session.Open(); err != nil {
		log.Fatal("failed to open discord session: ", err)
	}
	defer session.Close()

	log.Println("Syncing members from server")
	syncMembersFromServer(session)

	go func() {
		timer := time.NewTicker(12 * time.Hour)
		for range timer.C {
			log.Println("Performing scheduled sync")
			syncMembersFromServer(session)
		}
	}()

	log.Println("I'm running 😊")
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	<-sc
	log.Println("I'm closing 😢")
}

//go:embed migrations
var migrations embed.FS

func migrate(db *sql.DB) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS migrations (id INTEGER NOT NULL PRIMARY KEY, name TEXT UNIQUE);")
	if err != nil {
		return err
	}

	stmtCheck, err := db.Prepare("SELECT 1 FROM migrations WHERE name = ?")
	if err != nil {
		return err
	}
	defer stmtCheck.Close()

	stmtStore, err := db.Prepare("INSERT INTO migrations(name) VALUES (?)")
	if err != nil {
		return err
	}
	defer stmtStore.Close()

	migrationDirEntries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}

	migrationFiles := []string{}
	for _, migrationDirEntry := range migrationDirEntries {
		if migrationDirEntry.IsDir() {
			continue
		}
		migrationFiles = append(migrationFiles, migrationDirEntry.Name())
	}

	sort.Slice(migrationFiles, func(i, j int) bool {
		return strings.Compare(migrationFiles[i], migrationFiles[j]) <= 0
	})

	for _, migrationFile := range migrationFiles {
		result := stmtCheck.QueryRow(migrationFile)
		var i int
		err := result.Scan(&i)
		if err == nil {
			// already migrated
			continue
		}
		if err != sql.ErrNoRows {
			// other unknown error
			return err
		}

		migrationSql, err := migrations.ReadFile(path.Join("migrations", migrationFile))
		if err != nil {
			return err
		}

		log.Printf("[migration] RUN %v", migrationFile)
		_, err = db.Exec(string(migrationSql))
		if err != nil {
			return err
		}
		_, err = stmtStore.Exec(migrationFile)
		if err != nil {
			return err
		}
		log.Printf("[migration] FIN %v", migrationFile)
	}

	return nil
}

func ready(s *discordgo.Session, event *discordgo.Ready) {
	s.UpdateGameStatus(0, "hello")
}

func guildMemberAdd(s *discordgo.Session, m *discordgo.GuildMemberAdd) {
	if m.GuildID != guildID || m.User == nil {
		return
	}
	// log.Printf("received member added event: %v", m.User.ID)
	// memberAdded(s, m.User.ID, m.User.Username, m.User.Discriminator)
	memberAdded(s, m.User.ID, discordUser{
		username:      m.User.Username,
		discriminator: m.User.Discriminator,
	})
}

func guildMemberRemove(s *discordgo.Session, m *discordgo.GuildMemberRemove) {
	if m.GuildID != guildID || m.User == nil {
		return
	}
	// log.Printf("received member remove event: %v", m.User.ID)
	memberRemoved(s, m.User.ID)
}

func memberAdded(s *discordgo.Session, discordID string, user discordUser) {
	knownMemberStateLock.Lock()
	defer knownMemberStateLock.Unlock()
	memberAddedLocked(s, discordID, user)
}

func memberAddedLocked(s *discordgo.Session, discordID string, user discordUser) {
	_, exists := knownMemberState[discordID]
	if exists {
		return
	}
	_, err := stmtAdd.Exec(discordID, user.username, user.discriminator)
	if err != nil {
		log.Fatalf("failed to insert member '%v' to persistent storage: %v", err, discordID)
	}
	knownMemberState[discordID] = user
	if !knownMemberStateEmpty {
		if user.username == "" && user.discriminator == "" {
			_, err = s.ChannelMessageSend(channelID, fmt.Sprintf("<@%v> joined the server", discordID))
		} else {
			_, err = s.ChannelMessageSend(channelID, fmt.Sprintf("<@%v> (%v#%v) joined the server", discordID, user.username, user.discriminator))
		}
		if err != nil {
			log.Fatalf("failed to send message about '%v' joining server: %v", discordID, err)
		}
		log.Printf("messaged about '%v' joining", discordID)
	}
}

func memberUpdatedLocked(s *discordgo.Session, discordID string, user discordUser) {
	_, err := stmtUpdate.Exec(user.username, user.discriminator, discordID)
	if err != nil {
		log.Fatalf("failed to update member '%v' in persistent storage: %v", err, discordID)
	}
	knownMemberState[discordID] = user
}

func memberRemoved(s *discordgo.Session, discordID string) {
	knownMemberStateLock.Lock()
	defer knownMemberStateLock.Unlock()
	memberRemovedLocked(s, discordID)
}

func memberRemovedLocked(s *discordgo.Session, discordID string) {
	user, exists := knownMemberState[discordID]
	if !exists {
		return
	}
	_, err := stmtRemove.Exec(discordID)
	if err != nil {
		log.Fatalf("failed to delete member '%v' from persistent storage: %v", err, discordID)
	}
	delete(knownMemberState, discordID)
	if !knownMemberStateEmpty {
		if user.username == "" && user.discriminator == "" {
			_, err = s.ChannelMessageSend(channelID, fmt.Sprintf("<@%v> left the server", discordID))
		} else {
			_, err = s.ChannelMessageSend(channelID, fmt.Sprintf("<@%v> (%v#%v) left the server", discordID, user.username, user.discriminator))
		}
		if err != nil {
			log.Fatalf("failed to send message about '%v' leaving server: %v", discordID, err)
		}
		log.Printf("messaged about '%v' leaving", discordID)
	}
}

func syncMembersFromServer(s *discordgo.Session) {
	knownMemberStateLock.Lock()
	defer knownMemberStateLock.Unlock()

	// we'll remove members from this as we go
	// any members left at the end are no longer in the server
	knownMemberStateClone := make(map[string]interface{}, len(knownMemberState))
	for discordID := range knownMemberState {
		knownMemberStateClone[discordID] = nil
	}

	var (
		after   string
		members []*discordgo.Member
		err     error
	)
	const limit = 1000
	for {
		members, err = s.GuildMembers(guildID, after, limit)
		if err != nil {
			log.Fatalf("failed fetching guild members after '%v': %v", after, err)
		}

		for _, member := range members {
			if member.User == nil {
				continue
			}
			memberUser := discordUser{
				username:      member.User.Username,
				discriminator: member.User.Discriminator,
			}
			user, exists := knownMemberState[member.User.ID]
			if exists {
				if user.username != memberUser.username || user.discriminator != memberUser.discriminator {
					memberUpdatedLocked(s, member.User.ID, memberUser)
				}
			} else {
				memberAddedLocked(s, member.User.ID, memberUser)
			}
			delete(knownMemberStateClone, member.User.ID)
		}

		// less than limit returned - we're done!
		if len(members) < limit {
			break
		}

		// could be more
		after = members[len(members)-1].User.ID
	}

	// these users weren't found in the server, assume we missed their leave event
	for discordID := range knownMemberStateClone {
		memberRemovedLocked(s, discordID)
	}

	// member state is known now, notifications are allowed
	knownMemberStateEmpty = false
}
