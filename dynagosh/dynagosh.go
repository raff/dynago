//
// An interactive shell for DynamoDB
//
package main

import (
	"github.com/raff/dynago"

	"code.google.com/p/gcfg"
	"github.com/gobs/args"
	"github.com/gobs/cmd"
	"github.com/gobs/httpclient"
	"github.com/gobs/pretty"

	"encoding/json"
	"errors"
	//"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
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
		// display prompt
		Prompt bool
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

type RangeParam struct {
	Operator  string
	Condition *RangeCondition
	IsBool    bool
}

func (cond *RangeParam) Set(value string) error {
	if len(cond.Condition.Operator) > 0 {
		return errors.New("range-condition value already set")
	}

	cond.Condition.Operator = cond.Operator
	cond.Condition.Value = value
	return nil
}

func (cond *RangeParam) String() string {
	if len(cond.Condition.Value) > 0 {
		return cond.Condition.Value
	} else {
		return "{value}"
	}
}

func (cond *RangeParam) IsBoolFlag() bool {
	return cond.IsBool
}

type ScanFilter struct {
	Op      string
	Filters *dynago.AttrCondition
}

func (filter *ScanFilter) Set(value string) error {
	// value should be in one of the following formats
	// name - attr:{name}, type:S, val:""
	// name:value - attr:{name}, type:S, val:{stringvalue}
	// name:type:value - attr:{name}, type:{type}, val:{value}

	parts := strings.SplitN(value, ":", 3)
	if len(parts) == 0 {
		return errors.New("missing-value")
	}

	attr := parts[0]
	typ := "S"
	val := ""

	if len(parts) > 1 {
		typ = parts[1]
	}
	if len(parts) > 2 {
		val = parts[2]
	}

	switch filter.Op {
	case "NULL", "NOT_NULL":
		(*filter.Filters)[attr] = dynago.MakeCondition(filter.Op, typ)
	default:
		(*filter.Filters)[attr] = dynago.MakeCondition(filter.Op, typ, val)
	}
	return nil
}

func (filter *ScanFilter) String() string {
	return "name:type:value"
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
	if config.Dynago.Prompt {
		commander.Prompt = "dynagosh> "
	} else {
		commander.Prompt = "\n"
	}

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

			tableName, args := args[0], args[1:]
			table, err := db.GetTable(tableName)

			if err != nil {
				fmt.Println(err)
				return
			}

			hashKey, args := args[0], args[1:]
			var rangeKey interface{}

			// XXX: here we are forced to pass a rangeKey in order to get to "attributes"
			//      we should probably add a --rangeKey parameter or have a "splittable" single key
			if len(args) > 0 {
				rangeKey, args = args[0], args[1:]
			}

			var attributes []string

			if len(args) > 0 {
				attributes = args
			}

			if item, consumed, err := table.GetItem(hashKey, rangeKey, attributes, false, true); err != nil {
				fmt.Println(err)
			} else if len(attributes) > 0 {
				for _, n := range attributes {
					fmt.Print(" ", item[n])
				}
				fmt.Println()
			} else {
				pretty.PrettyPrint(item)
				fmt.Println("consumed:", consumed)
			}

			return
		}})

	commander.Add(cmd.Command{"query",
		`
		query [--table=tablename] [--limit=pagesize] [--next] [--count] [--consumed] --hash hash-key-value [--range[-rangeop] range-key-value]
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

			flags.Var(&RangeParam{"EQ", &rangeCond, false}, "range", "range-key value")
			flags.Var(&RangeParam{"EQ", &rangeCond, false}, "range-eq", "range-key equal value")
			flags.Var(&RangeParam{"NE", &rangeCond, false}, "range-ne", "range-key not-equal value")
			flags.Var(&RangeParam{"LE", &rangeCond, false}, "range-le", "range-key less-or-equal value")
			flags.Var(&RangeParam{"LT", &rangeCond, false}, "range-lt", "range-key less-than value")
			flags.Var(&RangeParam{"GE", &rangeCond, false}, "range-ge", "range-key less-or-equal value")
			flags.Var(&RangeParam{"GT", &rangeCond, false}, "range-gt", "range-key less-than value")
			flags.Var(&RangeParam{"CONTAINS", &rangeCond, false}, "range-contains", "range-key contains value")
			flags.Var(&RangeParam{"NOT_CONTAINS", &rangeCond, false}, "range-not-contains", "range-key not-contains value")
			flags.Var(&RangeParam{"BEGINS_WITH", &rangeCond, false}, "range-begins-with", "range-key begins-with value")
			flags.Var(&RangeParam{"NULL", &rangeCond, true}, "range-null", "range-key is null")
			flags.Var(&RangeParam{"NOT_NULL", &rangeCond, true}, "range-not-null", "range-key is-not null")

			if err := args.ParseFlags(flags, line); err != nil {
				return
			}

			args := flags.Args()

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
				switch rangeCond.Operator {
				case "NULL", "NOT_NULL":
					query = query.WithAttrCondition(table.RangeKey().Condition(rangeCond.Operator))
				default:
					query = query.WithAttrCondition(table.RangeKey().Condition(rangeCond.Operator, rangeCond.Value))
				}
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
		scan [--table=tablename] [--limit=pagesize] [--next] [--count] [--consumed] [--format=pretty|compact|json] [--segment=n --total=m]
		`,
		func(line string) (stop bool) {
			flags := args.NewFlags("scan")

			tableName := flags.String("table", "", "table name")
			limit := flags.Int("limit", 0, "maximum number of items per page")
			count := flags.Bool("count", false, "only return item count")
			next := flags.Bool("next", false, "get next page")
			cons := flags.Bool("consumed", false, "return consumed capacity")
			segment := flags.Int("segment", 0, "segment number")
			total := flags.Int("total", 0, "total segment")
			delay := flags.String("delay", "0ms", "delay (as duration string) between scan requests")
			format := flags.String("format", "pretty", "output format: pretty, compact or json")
			all := flags.Bool("all", false, "fetch all entries")

			filters := make(dynago.AttrCondition)

			flags.Var(&ScanFilter{"EQ", &filters}, "eq", "attr equal value")
			flags.Var(&ScanFilter{"NE", &filters}, "ne", "attr not-equal value")
			flags.Var(&ScanFilter{"LE", &filters}, "le", "attr less-or-equal value")
			flags.Var(&ScanFilter{"LT", &filters}, "lt", "attr less-than value")
			flags.Var(&ScanFilter{"GE", &filters}, "ge", "attr less-or-equal value")
			flags.Var(&ScanFilter{"GT", &filters}, "gt", "attr less-than value")
			flags.Var(&ScanFilter{"CONTAINS", &filters}, "contains", "attr contains value")
			flags.Var(&ScanFilter{"NOT_CONTAINS", &filters}, "not-contains", "attr not-contains value")
			flags.Var(&ScanFilter{"BEGINS_WITH", &filters}, "begins-with", "attr begins-with value")
			flags.Var(&ScanFilter{"NULL", &filters}, "null", "attr is null")
			flags.Var(&ScanFilter{"NOT_NULL", &filters}, "not-null", "attr is-not null")

			if err := args.ParseFlags(flags, line); err != nil {
				return
			}

			table := selectedTable

			if len(*tableName) > 1 {
				if t, err := db.GetTable(*tableName); err != nil {
					log.Println(err)
					return
				} else {
					table = t
				}
			} else if table == nil {
				fmt.Println("no table selected")
				return
			}

			scan := dynago.ScanTable(table)

			if *segment != 0 || *total != 0 {
				scan = scan.WithSegment(*segment, *total)
			}

			if len(filters) > 0 {
				scan = scan.WithFilters(filters)
			}

			if *limit > 0 {
				scan = scan.WithLimit(*limit)
			}

			if *cons {
				scan = scan.WithConsumed(true)
			}

			scanDelay, _ := time.ParseDuration(*delay)

			if *count {
				if totalCount, scanCount, consumed, err := scan.CountWithDelay(db, scanDelay); err != nil {
					log.Println(err)
				} else {
					fmt.Println("count:", totalCount)
					fmt.Println("scan count:", scanCount)
					if *cons {
						fmt.Println("consumed:", consumed)
					}
				}

				return
			}

			if *all {
				*next = true
			}

			for {
				if *next {
					scan = scan.WithStartKey(nextKey)
				}

				if config.Dynago.Debug {
					log.Printf("request: %#v\n", scan)
				}

				items, lastKey, consumed, err := scan.Exec(db)
				if err != nil {
					log.Println(err)

					if !strings.Contains(err.Error(), "ConnectEx") {
						break
					}
				} else {
					if *format == "compact" {
						p := &pretty.Pretty{Indent: "", Out: os.Stdout, NilString: "null"}
						for _, i := range items {
							p.Print(i)
						}
					} else if *format == "json" {
						j := json.NewEncoder(os.Stdout)
						for _, i := range items {
							j.Encode(i)
						}
					} else {
						pretty.PrettyPrint(items)
					}

					//if *cons {
					//	fmt.Println("consumed:", consumed)
					//}

					nextKey = lastKey
				}

				if (!*all) || len(nextKey) == 0 {
					break
				}

				if scanDelay > 0 {
					log.Println(nextKey, consumed)
					time.Sleep(scanDelay)
				}
			}

			return
		}})

	commander.Commands["ls"] = commander.Commands["list"]
	commander.Commands["drop"] = commander.Commands["delete"]

	commander.CmdLoop()
}
