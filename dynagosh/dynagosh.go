package main

import (
	"../../dynago"

	"code.google.com/p/gcfg"
	"github.com/gobs/cmd"
	"github.com/gobs/pretty"

	"fmt"
	"log"
	"os"
	"path"
	"strings"
)

const (
	CONFIG_FILE  = ".dynagorc"
	HISTORY_FILE = ".dynago_history"
)

// the configuration should look like the following
// (with multiple profiles and a selected one)
//
// [dynago]
// profile=xxx
//
// [profile "xxx"]
// region=us-west-1
// accessKey=XXXXXXXX
// secretKey=YYYYYYYY

type Config struct {
	Dynago struct {
		// define default profile
		Profile string
	}

	// list of named profiles
	Profile map[string]*struct {
		Region    string
		AccessKey string
		SecretKey string
	}
}

func ReadConfig(configFile string, config *Config) {
	// configFile in current directory or full path
	if _, err := os.Stat(configFile); err != nil {
		if strings.Contains(configFile, "/") {
			return
		}

		// configFile in home directory
		configFile = path.Join(os.Getenv("HOME"), configFile)
		if _, err := os.Stat(configFile); err != nil {
			return
		}
	}

	err := gcfg.ReadFileInto(config, configFile)
	if err != nil {
		log.Fatal(err)
	}
}

func CompletionFunction(text string, line string, start, stop int) []string {
	return nil
}

func main() {
	var config Config
	ReadConfig(CONFIG_FILE, &config)

	selected := config.Dynago.Profile

	if len(os.Args) > 1 {
		// there is at least one parameter:
		// override the selected profile
		selected = os.Args[1]
	}

	profile := config.Profile[selected]
	if profile == nil {
		log.Fatal("no profile selected")
	}

	db := dynago.NewDBClient()

	if len(profile.Region) > 0 {
		db.WithRegion(profile.Region)
	}

	if len(profile.AccessKey) > 0 {
		db.WithCredentials(profile.AccessKey, profile.SecretKey)
	}

	commander := &cmd.Cmd{HistoryFile: HISTORY_FILE, Complete: CompletionFunction, EnableShell: true}
	commander.Prompt = "dynagosh> "
	commander.Init()

	commander.Add(cmd.Command{"config",
		`
		config : display current configuration
		`,
		func(string) (stop bool) {
			pretty.PrettyPrint(config)
			return
		}})

	commander.Add(cmd.Command{"list",
		`
                list : display list of available tables
                `,
		func(string) (stop bool) {
			tables, err := db.ListTables()

			if err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println("Available tables")

			for _, tableName := range tables {
				fmt.Println("  ", tableName)
			}

			return
		}})

	commander.Add(cmd.Command{"describe",
		`
                describe {table} : display table configuration
                `,
		func(line string) (stop bool) {
			tableName := line
			table, err := db.DescribeTable(tableName)
			if err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(table)
			}

			return
		}})

	commander.Commands["ls"] = commander.Commands["list"]

	commander.CmdLoop()
}
