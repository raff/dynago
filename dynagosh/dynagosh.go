package main

import (
	"../../dynago"

	"code.google.com/p/gcfg"
        "github.com/gobs/cmd"
        "github.com/gobs/pretty"

	"fmt"
	"log"
	"os"
)

const (
    CONFIG_FILE = ".dynagorc"
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

func CompletionFunction(text string, line string, start, stop int) []string {
    return nil
}

func main() {
	var config Config

	err := gcfg.ReadFileInto(&config, CONFIG_FILE)
	if err != nil {
		log.Fatal(err)
	}

	selected := config.Dynago.Profile

	if len(os.Args) > 1 {
		// there is at least one parameter:
		// override the selected profile
		selected = os.Args[1]
	}

	profile := config.Profile[selected]

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
