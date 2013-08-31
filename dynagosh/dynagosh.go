//
// An interactive shell for DynamoDB
//
package main

import (
	"../../dynago"

	"code.google.com/p/gcfg"
	"github.com/gobs/args"
	"github.com/gobs/cmd"
	"github.com/gobs/httpclient"
	"github.com/gobs/pretty"

	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
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
		// enable request debugging
		Debug bool
	}

	// list of named profiles
	Profile map[string]*struct {
		Region    string
		AccessKey string
		SecretKey string
	}
}

// Look for configFile in current directory or home directory.
// No configuration file is NOT an error.
// A malformed configuration file is a FATAL error.

func ReadConfig(configFile string, config *Config) *Config {
	if config == nil {
		config = &Config{}
	}

	// configFile in current directory or full path
	if _, err := os.Stat(configFile); err != nil {
		if strings.Contains(configFile, "/") {
			return config
		}

		// configFile in home directory
		configFile = path.Join(os.Getenv("HOME"), configFile)
		if _, err := os.Stat(configFile); err != nil {
			return config
		}
	}

	err := gcfg.ReadFileInto(config, configFile)
	if err != nil {
		log.Fatal(err)
	}

	return config
}

var (
	// this hold the current list of table names, to be used by the CompletionFunction
	table_list []string
)

func add_to_list(table string) {
	table_list = append(table_list, table)
}

func remove_from_list(table string) {
	for i, t := range table_list {
		if t == table {
			table_list = append(table_list[:i], table_list[i+1:]...)
			return
		}
	}
}

// return list of table names that match the input pattern (table name starts with "text")
func CompletionFunction(text string, line string, start, stop int) []string {
	if len(table_list) > 0 {
		matches := make([]string, 0, len(table_list))

		for _, w := range table_list {
			if strings.HasPrefix(w, text) {
				matches = append(matches, w)
			}
		}

		return matches
	}

	return nil
}

type RangeCondition struct {
	Operator string
	Value    string
}

func (cond *RangeCondition) Set(value string) error {
	if len(cond.Value) > 0 {
		return errors.New("range-condition value already set")
	}

	cond.Value = value
	return nil
}

func (cond *RangeCondition) String() string {
	return fmt.Sprintf("%s: %v", cond.Operator, cond.Value)
}

func (cond *RangeCondition) SetOperator(f *flag.Flag) {
	if f.Name == "range" {
		cond.Operator = "EQ"
	} else if strings.HasPrefix(f.Name, "range-") {
		cond.Operator = strings.ToUpper(f.Name[6:])
	}
}

func main() {
	var nextKey dynago.AttributeNameValue
	var selectedTable *dynago.TableInstance

	config := ReadConfig(CONFIG_FILE, nil)
	selected := config.Dynago.Profile

	if len(os.Args) > 1 {
		// there is at least one parameter:
		// override the selected profile
		selected = os.Args[1]
	}

	profile := config.Profile[selected]
	if profile == nil {
		log.Fatal("no profile for ", selected)
	}

	if config.Dynago.Debug {
		httpclient.StartLogging(true, true)
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

			if len(tables) > 0 {
				table_list = tables
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

	commander.Add(cmd.Command{"use",
		`
                use {table} : select table for queries
                `,
		func(line string) (stop bool) {
			tableName := line
			table, err := db.GetTable(tableName)
			if err != nil {
				fmt.Println(err)
			} else {
				selectedTable = table
				commander.Prompt = "dynagosh: " + tableName + "> "
			}

			return
		}})

	commander.Add(cmd.Command{"create",
		`
		create {tablename} hashKey:hashType [rangeKey:rangeType] [readCapacity] [writeCapacity]
		`,
		func(line string) (stop bool) {
			args := args.GetArgs(line)

			if len(args) < 2 {
				fmt.Println("not enough arguments")
				return
			}

			tableName := args[0]

			hashKey := &dynago.AttributeDefinition{AttributeType: dynago.STRING_ATTRIBUTE}
			var rangeKey *dynago.AttributeDefinition
			rc := 5
			wc := 5

			if strings.Contains(args[1], ":") {
				parts := strings.Split(args[1], ":")
				hashKey.AttributeName = parts[0]
				hashKey.AttributeType = parts[1]
			} else {
				hashKey.AttributeName = args[1]
			}

			if len(args) > 2 {
				rangeKey := &dynago.AttributeDefinition{AttributeType: dynago.STRING_ATTRIBUTE}

				if strings.Contains(args[2], ":") {
					parts := strings.Split(args[2], ":")
					rangeKey.AttributeName = parts[0]
					rangeKey.AttributeType = parts[1]
				} else {
					rangeKey.AttributeName = args[2]
				}
			}

			if table, err := db.CreateTable(tableName, hashKey, rangeKey, rc, wc); err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(table)
				add_to_list(tableName)
			}

			return
		}})

	commander.Add(cmd.Command{"delete",
		`
                delete {table} : delete table
                `,
		func(line string) (stop bool) {
			tableName := line
			table, err := db.DeleteTable(tableName)
			if err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(table)
				remove_from_list(tableName)
			}

			return
		}})

	commander.Add(cmd.Command{"update",
		`
		update {tablename} readCapacity writeCapacity
		`,
		func(line string) (stop bool) {
			args := args.GetArgs(line)

			if len(args) < 2 {
				fmt.Println("not enough arguments")
				return
			}

			tableName := args[0]
			table, err := db.DescribeTable(tableName)

			if err != nil {
				fmt.Println(err)
				return
			}

			rc := -1 // table.ProvisionedThroughput.ReadCapacityUnits
			wc := -1 // table.ProvisionedThroughput.WriteCapacityUnits

			if v, err := strconv.Atoi(args[1]); err == nil {
				rc = v
			}

			if len(args) > 2 {
				if v, err := strconv.Atoi(args[2]); err == nil {
					wc = v
				}
			}

			if rc <= 0 && wc <= 0 {
				fmt.Println("no valid value for rc or wc")
				return
			}

			if rc <= 0 {
				rc = table.ProvisionedThroughput.ReadCapacityUnits
			}

			if wc <= 0 {
				wc = table.ProvisionedThroughput.WriteCapacityUnits
			}

			if table, err := db.UpdateTable(tableName, rc, wc); err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(table)
			}

			return
		}})

	commander.Add(cmd.Command{"get",
		`
		get {tablename} {hashKey} [rangeKey] [attributes]
		`,
		func(line string) (stop bool) {
			args := args.GetArgs(line)

			if len(args) < 2 {
				fmt.Println("not enough arguments")
				return
			}

			tableName := args[0]
			table, err := db.GetTable(tableName)

			if err != nil {
				fmt.Println(err)
				return
			}

			hashKey := args[1]
			var rangeKey string

			if len(args) > 2 {
				rangeKey = args[2]
			}

			var attributes []string

			if len(args) > 3 {
				attributes = args[3:]
			}

			if item, consumed, err := table.GetItem(hashKey, rangeKey, attributes, false, true); err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(item)
				fmt.Println("consumed:", consumed)
			}

			return
		}})

	commander.Add(cmd.Command{"query",
		`
		query [--table=tablename] [--limit=pagesize] [--next] [--count] [--consumed] --hash hash-key-value [--range range-key-value]
		`,
		func(line string) (stop bool) {
			flags := args.NewFlags("query")

			tableName := flags.String("table", "", "table name")
			limit := flags.Int("limit", 0, "maximum number of items per page")
			count := flags.Bool("count", false, "only return item count")
			next := flags.Bool("next", false, "get next page")
			consumed := flags.Bool("consumed", false, "return consumed capacity")

			hashKey := flags.String("hash", "", "hash-key value")

			var rangeCond RangeCondition

			flags.Var(&rangeCond, "range", "range-key value")
			flags.Var(&rangeCond, "range-eq", "range-key equal value")
			flags.Var(&rangeCond, "range-ne", "range-key not-equal value")
			flags.Var(&rangeCond, "range-le", "range-key less-or-equal value")
			flags.Var(&rangeCond, "range-lt", "range-key less-than value")
			flags.Var(&rangeCond, "range-ge", "range-key less-or-equal value")
			flags.Var(&rangeCond, "range-gt", "range-key less-than value")

			args.ParseFlags(flags, line)
			args := flags.Args()

			flags.Visit(rangeCond.SetOperator)

			table := selectedTable

			if len(*tableName) > 1 {
				if t, err := db.GetTable(*tableName); err != nil {
					fmt.Println(err)
					return
				} else {
					table = t
				}
			} else if table == nil {
				fmt.Println("no table selected")
				return
			}

			if len(*hashKey) < 1 {
				if len(args) < 1 {
					fmt.Println("not enough arguments")
					return
				}

				*hashKey = args[0]

				if len(rangeCond.Operator) < 1 && len(args) > 1 {
					rangeCond.Operator = "EQ"
					rangeCond.Value = args[1]
				}
			}

			query := table.Query(*hashKey)

			if len(rangeCond.Operator) > 0 {
				query = query.WithAttrCondition(table.RangeKey().Condition(rangeCond.Operator, rangeCond.Value))
			}

			if *limit > 0 {
				query = query.WithLimit(*limit)
			}

			if *count {
				query = query.WithSelect(dynago.SELECT_COUNT)
			}

			if *next {
				query = query.WithStartKey(nextKey)
			}

			if *consumed {
				query = query.WithConsumed(true)
			}

			if items, lastKey, consumed, err := query.Exec(nil); err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(items)
				fmt.Println("consumed:", consumed)

				nextKey = lastKey
			}

			return
		}})

	commander.Add(cmd.Command{"scan",
		`
		scan [--table=tablename] [--limit=pagesize] [--next] [--count] [--consumed] [--segment=n --total=m]
		`,
		func(line string) (stop bool) {
			flags := args.NewFlags("scan")

			tableName := flags.String("table", "", "table name")
			limit := flags.Int("limit", 0, "maximum number of items per page")
			count := flags.Bool("count", false, "only return item count")
			next := flags.Bool("next", false, "get next page")
			consumed := flags.Bool("consumed", false, "return consumed capacity")
			segment := flags.Int("segment", 0, "segment number")
			total := flags.Int("total", 0, "total segment")

			args.ParseFlags(flags, line)
			//args := flags.Args()

			table := selectedTable

			if len(*tableName) > 1 {
				if t, err := db.GetTable(*tableName); err != nil {
					fmt.Println(err)
					return
				} else {
					table = t
				}
			} else if table == nil {
				fmt.Println("no table selected")
				return
			}

			scan := dynago.ScanTable(table).
				WithSegment(*segment, *total)

			if *limit > 0 {
				scan = scan.WithLimit(*limit)
			}

			if *count {
				scan = scan.WithSelect(dynago.SELECT_COUNT)
			}

			if *next {
				scan = scan.WithStartKey(nextKey)
			}

			if *consumed {
				scan = scan.WithConsumed(true)
			}

			if items, lastKey, consumed, err := scan.Exec(db); err != nil {
				fmt.Println(err)
			} else {
				pretty.PrettyPrint(items)
				fmt.Println("consumed:", consumed)

				nextKey = lastKey
			}

			return
		}})

	commander.Commands["ls"] = commander.Commands["list"]
	commander.Commands["drop"] = commander.Commands["delete"]

	commander.CmdLoop()
}
