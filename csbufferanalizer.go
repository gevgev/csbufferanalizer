package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	inFileName               string
	dirName                  string
	inExtension              string
	diagnostics              bool
	outputFormat             string
	outputFileName           string
	concurrency              int
	verbose                  bool
	supress                  bool
	singleFileMode           bool
	primetimeOnly            bool
	cummulativePrimetimeOnly bool
	vodLogOn                 bool
	eventSequenceLogOnly     bool
	appName                  string
)

const (
	version      = "0.01"
	txtOutput    = "csv"
	UTC_GPS_Diff = 315964800
	// iGuide R31 buff size
	BuffWaterMarkSize = 750
	rawExt            = "raw"
)

func init() {
	flagFileName := flag.String("f", "", "Input `filename` to process")
	flagDirName := flag.String("d", "", "Working `directory` for input files, default extension *.raw")
	flagExtension := flag.String("x", rawExt, "Input files `extension`: raw, cs")
	flagDiagnostics := flag.Bool("t", false, "Turns `diagnostic` messages On")
	flagOutputFormat := flag.String("s", txtOutput, "`Output format`s: txt, json, xml")
	flagOutputFile := flag.String("o", "output", "`Output filename`")
	flagConcurrency := flag.Int("c", 100, "The number of files to process `concurrent`ly")
	flagVerbose := flag.Bool("v", false, "`Verbose`: outputs to the screen")
	flagSupress2am := flag.Bool("S", false, "`Supress`: 2am-3am diagnostics messages")
	flagPrimetime := flag.Bool("P", false, "`Primetime`: 8pm-11pm events only")
	flagCombinedPrimetime := flag.Bool("PC", false, "`Cumulative Primetime`: 8pm-11pm events only cummulative single file")
	flagVod := flag.Bool("VOD", false, "Create the log(s) for `VOD` activity")
	flagEventSequenceLogOnly := flag.Bool("L", false, "Events sequence `log`")

	flag.Parse()
	if flag.Parsed() {
		inFileName = *flagFileName
		dirName = *flagDirName
		inExtension = *flagExtension
		diagnostics = *flagDiagnostics
		outputFormat = *flagOutputFormat
		outputFileName = *flagOutputFile
		concurrency = *flagConcurrency
		verbose = *flagVerbose
		supress = *flagSupress2am
		primetimeOnly = *flagPrimetime
		cummulativePrimetimeOnly = *flagCombinedPrimetime
		vodLogOn = *flagVod
		eventSequenceLogOnly = *flagEventSequenceLogOnly

		appName = os.Args[0]
		if inFileName == "" && dirName == "" && len(os.Args) == 2 {
			inFileName = os.Args[1]
		}
	} else {
		usage()
	}
	initEventNames()
}

func usage() {
	fmt.Printf("%s, ver. %s\n", appName, version)
	fmt.Println("Command line:")
	fmt.Printf("\tprompt$>%s <filename>\n", appName)
	fmt.Printf("\tprompt$>%s -f <filename> -d <dir> -o <outputfile> -s <outFormat> -t -v -x <extension>\n", appName)
	fmt.Println("Provide either file or dir. Dir takes over file, if both provided")
	flag.Usage()
	os.Exit(-1)
}

func convertToTime(timestampS string) time.Time {
	timestamp, err := strconv.ParseInt(timestampS, 16, 64)
	//fmt.Println(timestampS, timestamp)
	if err == nil {
		timestamp += UTC_GPS_Diff
		//fmt.Println(timestampS, timestamp)

		t := time.Unix(timestamp, 0)
		return t
	}
	//else {
	//fmt.Println("Error:", err)
	//}
	return time.Time{}
}

type Command struct {
	cmd        string
	name       string
	diagnostic bool
}

var commandsList = []Command{
	{"41", "`A`Ad Display", false},
	{"42", "`B`Button Config", true},
	{"43", "`C`Channel Change (verbose)", false},
	{"63", "`c`Channel Change (brief)", false},
	{"45", "`E`Program Event", false},
	{"46", "`F`Favorite", false},
	{"47", "`G`VOD Category", false},
	{"48", "`H`Highlight", false},
	{"49", "`I`Info Screen", false},
	{"4B", "`K`Key Press", false},
	{"4C", "`L`Lock", false},
	{"4D", "`M`Missing", false},
	{"4F", "`O`Option", false},
	{"50", "`P`Pulse", false},
	{"52", "`R`Reset", false},
	{"53", "`S`State Change", false},
	{"54", "`T`Turbo Key", false},
	{"55", "`U`Unit Ident.", true},
	{"56", "`V`Video Playback Session (non- OCAP)", false},
	{"58", "`X`Status", true},
	{"5A", "`Z`Menu Config.", true},
}

var (
	eventNames       map[string]string
	diagnosticEvents map[string]bool
)

func initEventNames() {
	eventNames = make(map[string]string, len(commandsList))
	diagnosticEvents = make(map[string]bool, 4)
	for _, cmd := range commandsList {
		eventNames[cmd.cmd] = cmd.name
		if cmd.diagnostic {
			diagnosticEvents[cmd.name] = cmd.diagnostic
		}
	}
}

func isDiagnosticEvent(cmd string) bool {
	_, ok := diagnosticEvents[cmd]
	return ok
}

func convertToString(str string) string {
	bytes, err := hex.DecodeString(str)
	if err == nil {
		return string(bytes)
	}
	return ""
}

func convertToLogName(cmd string) (string, error) {
	cmdStr, ok := eventNames[cmd]
	if !ok {
		return "", errors.New("Unknown Clickstream Code")
	}
	return cmdStr, nil
}

// just extract timestamp, device Id, and calculate event size
func parseEvent(line string, eventLogChan chan<- EventLogEntry, mso string) (timestamp time.Time, deviceId string, eventSize int, eventCode string, err error) {
	defer func() {
		if r := recover(); r != nil {
			timestamp = time.Now()
			err = errors.New("Parser time exception")
		}
	}()

	var receivedIndex, deviceIndex, clickstringIndex int

	tokens := strings.Split(line, " ")
	switch len(tokens) {
	case 2:
		receivedIndex = -1
		deviceIndex = 0
		clickstringIndex = 1
	case 3:
		receivedIndex = 0
		deviceIndex = 1
		clickstringIndex = 2
	default:
		if diagnostics {
			fmt.Println("Tokens were too many:", tokens)
		}
		return time.Now(), "", 0, "", errors.New("Wrong line format")
	}

	deviceId = tokens[deviceIndex]
	clickString := tokens[clickstringIndex]
	var received string
	if receivedIndex > -1 {
		received = tokens[receivedIndex]
	} else {
		received = "1900-01-01 00:00:00"
	}

	eventCode, err = convertToLogName(clickString[0:2])
	if err != nil {
		return
	}
	timestamp = convertToTime(clickString[2:10])
	eventSize = len(clickString) / 2
	err = nil

	if diagnostics {
		fmt.Printf("STB Id: %s \t eventCode: %s\t timeStamp: %v \t eventSize: %d\n",
			deviceId, eventCode, timestamp, eventSize)
	}

	if timestamp.After(time.Now()) {
		err = errors.New("Wrong date: " + timestamp.String())
	}

	if vodLogOn {
		if ok, logEntry := checkAndLogForVodActivity(eventCode, timestamp, received, deviceId, clickString, mso); ok == true {
			eventLogChan <- logEntry
		}
	} else if eventSequenceLogOnly {
		eventLogChan <- EventLogEntry{timestamp, received, deviceId, eventCode, mso}
	}
	return
}

func checkAndLogForVodActivity(eventCode string, timestamp time.Time, received string, deviceId string, clickString string, mso string) (bool, EventLogEntry) {
	switch eventCode {
	case "`G`VOD Category": // "47": // G
		return true, EventLogEntry{timestamp, received, deviceId, eventCode, mso}
	case "`I`Info Screen": // "49": // I
		if convertToString(clickString[10:12]) == "V" {
			return true, EventLogEntry{timestamp, received, deviceId, eventCode + " / Type V", mso}
		}
	case "`V`Video Playback Session (non- OCAP)": // "56": // V
		if convertToString(clickString[26:28]) == "V" {
			return true, EventLogEntry{timestamp, received, deviceId, eventCode + " / Source V", mso}
		}
	default:
		return false, EventLogEntry{}
	}
	return false, EventLogEntry{}
}

type EventLogEntry struct {
	timestamp time.Time
	received  string
	deviceId  string
	eventcode string
	mso       string
}

type ErrorLogEntry struct {
	fileName string
	lineNo   int
	line     string
	err      error
}

var errorsLog []ErrorLogEntry = []ErrorLogEntry{}

func logErrorEvent(fileName, line string, lineNo int, err error) {
	entry := ErrorLogEntry{
		fileName,
		lineNo,
		line,
		err,
	}
	errorsLog = append(errorsLog, entry)
}

func printErrorLogs() {
	file, err := os.Create("errorlog.txt")
	if err != nil {
		fmt.Println(err)
	}
	w := bufio.NewWriter(file)
	for _, logEntry := range errorsLog {
		fmt.Fprintf(w, "File: %s \t lineNo: %d\t Error:%s\nEntry:[%s]\n",
			logEntry.fileName, logEntry.lineNo, logEntry.err, logEntry.line)
	}
	w.Flush()
	file.Close()
}

// Single Clickstream package "sending"
type Package struct {
	timestamp time.Time
	deviceId  string
	eventCode string
}

func (pkg Package) String() string {
	return fmt.Sprintf("%v, %s, %s", pkg.timestamp, pkg.deviceId, pkg.eventCode)
}

type PackageList []Package

func (list PackageList) Len() int {
	return len(list)
}

func (list PackageList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list PackageList) Less(i, j int) bool {
	return list[i].timestamp.Before(list[j].timestamp)
}

// Emulate sending of one Clickstream Package
func Pack(timestamp time.Time, deviceId, eventCode string) Package {
	pkg := Package{}

	pkg.deviceId = deviceId
	pkg.timestamp = timestamp
	pkg.eventCode = eventCode

	if diagnostics {
		fmt.Println(pkg)
	}
	return pkg
}

func printOutputFile(packages PackageList) {
	sort.Sort(packages)

	file, err := os.Create(outputFileName + "." + outputFormat)
	if err != nil {
		fmt.Println(err)
	}
	w := bufio.NewWriter(file)
	for _, pkg := range packages {
		//		if primetimeOnly {
		//			if pkg.timestamp.Hour() >= 20.0 && pkg.timestamp.Hour() < 11.0 {
		//				fmt.Fprintln(w, pkg)
		//			}
		//		} else {
		fmt.Fprintln(w, pkg)
		//		}
	}
	w.Flush()
	file.Close()
}

func msoName(fileName string) string {
	mso := fileName[strings.LastIndex(fileName, "_")+1 : strings.LastIndex(fileName, ".")]
	return mso
}

func main() {
	startTime := time.Now()
	rand.Seed(int64(startTime.Second()))

	eventLogChan := make(chan EventLogEntry)
	var vodLog OrderedVodLogList

	go func() {
		for {
			logEntry, more := <-eventLogChan
			if more {
				vodLog = append(vodLog, logEntry)
			} else {
				return
			}
		}
	}()

	packages := []Package{}

	files := getFilesToProcess() //getFiles()

	totalEvents := 0
	// BufferSizes for devices
	bufferSize := make(map[string]int)

	for _, fileName := range files {
		if diagnostics {
			fmt.Println("Processing: ", fileName)
		}
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println("Error opening file: ", err)
			continue
		}
		mso := msoName(fileName)
		scanner := bufio.NewScanner(file)
		lineNo := 0
		for scanner.Scan() {
			line := scanner.Text()
			lineNo++
			if diagnostics {
				fmt.Println("Got next line: ", line)
			}
			timestamp, deviceId, eventSize, eventCode, err := parseEvent(line, eventLogChan, mso)

			if diagnostics {
				fmt.Println("Parsed into: ", timestamp, deviceId, eventSize, eventCode, err)
			}

			if err != nil {
				logErrorEvent(fileName, line, lineNo, err)
			} else {
				if _, ok := bufferSize[deviceId]; !ok {
					// First occurence
					bufferSize[deviceId] = rand.Intn(BuffWaterMarkSize)
				}
				if diagnostics {
					fmt.Println("Buff: ", bufferSize[deviceId])
					fmt.Println("Watermark:", BuffWaterMarkSize)
				}

				if supress && isDiagnosticEvent(eventCode) {
					// If supress diagnostic commands is requested, then ignore them
					if diagnostics {
						fmt.Println("Skipped:", timestamp, deviceId, eventSize, eventCode, err)
					}
				} else {
					if bufferSize[deviceId]+eventSize > BuffWaterMarkSize {
						pkg := Pack(timestamp, deviceId, eventCode)
						// Send a new package
						packages = append(packages, pkg)
						if diagnostics {
							fmt.Println("Sent package: ", pkg)
						}
						// Start the buffer from the beginning
						bufferSize[deviceId] = eventSize
					} else {
						bufferSize[deviceId] += eventSize
					}
				}
			}

		}
		totalEvents += lineNo
		file.Close()
	}

	// closing the eventLogChannel
	close(eventLogChan)

	if !eventSequenceLogOnly {
		printOutputFile(packages)
	}

	max, avg, total := printEventsPerSecond(packages)
	if vodLogOn {
		printVodLogEntries(vodLog)
	} else if eventSequenceLogOnly {
		printAllEvents(vodLog)
	}

	printErrorLogs()
	fmt.Println("Number of devices:\t", len(bufferSize))
	fmt.Println("Total events: \t\t", totalEvents)
	fmt.Println("Total packages:\t\t", len(packages))
	if len(packages) > 0 {
		fmt.Println("First package sent at: ", packages[0].timestamp)
		fmt.Println("Last  package sent at: ", packages[len(packages)-1].timestamp)
	} else {
		fmt.Println("No packages were sent")
	}
	fmt.Println("Error entries number: ", len(errorsLog))
	fmt.Println("Total reported at times: ", total)
	fmt.Printf("Max per second: %d at %v\n", max.numberOfEvents, max.timestamp)
	fmt.Println("Average per second: ", avg)
	fmt.Printf("Processed %d files in %v\n", len(files), time.Since(startTime))
}

func printAllEvents(eventsLog OrderedVodLogList) {

	if len(eventsLog) == 0 {
		fmt.Println("No events")
	} else {
		sort.Sort(eventsLog)
		// Now save this to a a single events log file

		file, err := os.Create("eventsLog-" + time.Now().Format("01 02 2006_15 04 05") + ".csv")
		if err != nil {
			fmt.Println(err)
		}

		w := bufio.NewWriter(file)
		for _, event := range eventsLog {
			fmt.Fprintf(w, "%v, %v, %v, %v, %v\n",
				event.timestamp, event.received, event.deviceId, event.eventcode, event.mso)
		}
		// Closing the file
		w.Flush()
		file.Close()
	}

}

func printVodLogEntries(vodLog OrderedVodLogList) {

	if len(vodLog) == 0 {
		fmt.Println("No VOD events")
	} else {
		sort.Sort(vodLog)
		// Now save this to a vod log file
		// This is going to be the first file name
		currentYear, currentMonth, currentDay := vodLog[0].timestamp.Date()

		file, err := os.Create(formateCurrentFileName("vodLog", currentYear, currentMonth, currentDay))
		if err != nil {
			fmt.Println(err)
		}

		w := bufio.NewWriter(file)
		for _, vodEntry := range vodLog {

			if !validateFileDate(currentYear, currentMonth, currentDay, vodEntry.timestamp) {
				// Close current file, open a new file - new date
				w.Flush()
				file.Close()

				currentYear, currentMonth, currentDay = vodEntry.timestamp.Date()

				file, err = os.Create(formateCurrentFileName("vodLog", currentYear, currentMonth, currentDay))
				if err != nil {
					fmt.Println(err)
				}
				w = bufio.NewWriter(file)
			}

			fmt.Fprintf(w, "%v, %v, %v\n",
				vodEntry.timestamp, vodEntry.received, vodEntry.deviceId, vodEntry.eventcode, vodEntry.mso)
		}
		// Closing the last file
		w.Flush()
		file.Close()
	}

}

type OrderedVodLogList []EventLogEntry

func (list OrderedVodLogList) Len() int {
	return len(list)
}

func (list OrderedVodLogList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list OrderedVodLogList) Less(i, j int) bool {
	return list[i].timestamp.Before(list[j].timestamp)
}

// Single Clickstream package "sending"
type TimepointType struct {
	timestamp      time.Time
	numberOfEvents int
}

func (tp TimepointType) String() string {
	return fmt.Sprintf("%v, %s", tp.timestamp, tp.numberOfEvents)
}

type TimepointTypeList []TimepointType

func (list TimepointTypeList) Len() int {
	return len(list)
}

func (list TimepointTypeList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list TimepointTypeList) Less(i, j int) bool {
	return list[i].timestamp.Before(list[j].timestamp)
}

func printEventsPerSecond(packages PackageList) (max TimepointType, avg int, total int) {
	eventsPerSecond := make(map[time.Time]int)

	for _, pkg := range packages {
		//		fmt.Println("Pkg timestamp: ", pkg.timestamp.Hour())

		if primetimeOnly {
			if pkg.timestamp.Hour() >= 20.0 && pkg.timestamp.Hour() < 23.0 {
				if _, ok := eventsPerSecond[pkg.timestamp]; ok {
					eventsPerSecond[pkg.timestamp]++
				} else {
					eventsPerSecond[pkg.timestamp] = 1
				}
			}
		} else if cummulativePrimetimeOnly {
			// We will ignore dates, only timestamps matter
			if pkg.timestamp.Hour() >= 20.0 && pkg.timestamp.Hour() < 23.0 {

				unifiedTimeStampVal := unifiedTimeStamp(pkg.timestamp)
				if _, ok := eventsPerSecond[unifiedTimeStampVal]; ok {
					eventsPerSecond[unifiedTimeStampVal]++
				} else {
					eventsPerSecond[unifiedTimeStampVal] = 1
				}
			}

		} else {
			if _, ok := eventsPerSecond[pkg.timestamp]; ok {
				eventsPerSecond[pkg.timestamp]++
			} else {
				eventsPerSecond[pkg.timestamp] = 1
			}
		}
	}

	var orderedEventsPerSecond TimepointTypeList
	for k, v := range eventsPerSecond {
		orderedEventsPerSecond = append(orderedEventsPerSecond, TimepointType{k, v})
	}

	sort.Sort(orderedEventsPerSecond)

	if len(orderedEventsPerSecond) == 0 {
		// Nothing to print
		if diagnostics {
			fmt.Println("No events were found for primetime")
		}
		return
	}

	if eventSequenceLogOnly {
		for _, points := range orderedEventsPerSecond {
			if points.numberOfEvents > max.numberOfEvents {
				max = points
			}
			avg += points.numberOfEvents
		}

	} else {
		// This is going to be the first file name
		currentYear, currentMonth, currentDay := orderedEventsPerSecond[0].timestamp.Date()

		file, err := os.Create(formateCurrentFileName("eventsPerSecond", currentYear, currentMonth, currentDay))
		if err != nil {
			fmt.Println(err)
		}

		w := bufio.NewWriter(file)
		for _, points := range orderedEventsPerSecond {

			if !validateFileDate(currentYear, currentMonth, currentDay, points.timestamp) {
				// Close current file, open a new file - new date
				w.Flush()
				file.Close()

				currentYear, currentMonth, currentDay = points.timestamp.Date()

				file, err = os.Create(formateCurrentFileName("eventsPerSecond", currentYear, currentMonth, currentDay))
				if err != nil {
					fmt.Println(err)
				}
				w = bufio.NewWriter(file)
			}

			fmt.Fprintf(w, "%v, %d\n", points.timestamp, points.numberOfEvents)
			if points.numberOfEvents > max.numberOfEvents {
				max = points
			}

			avg += points.numberOfEvents
		}
		// Closing the last file
		w.Flush()
		file.Close()
	}

	if len(orderedEventsPerSecond) > 0 {
		avg = avg / len(orderedEventsPerSecond)
	}

	total = len(orderedEventsPerSecond)

	return
}

// Drops the date part, make everything time of 01/01/2016
func unifiedTimeStamp(timestamp time.Time) time.Time {
	hour, min, sec := timestamp.Clock()

	unifiedDateTime := time.Date(2016, 1, 1, hour, min, sec, 0, time.Local)
	return unifiedDateTime
}

// Compare current date and the date for the next event's timestamp
func validateFileDate(currentYear int, currentMonth time.Month, currentDay int, timestamp time.Time) bool {
	year, month, day := timestamp.Date()
	return (year == currentYear && month == currentMonth && day == currentDay)
}

// filename for the current date
func formateCurrentFileName(fileprefix string, currentYear int, currentMoth time.Month, currentDay int) string {
	fileName := fmt.Sprintf("%s-%04d-%02d-%02d.csv", fileprefix, currentYear, int(currentMoth), currentDay)
	if diagnostics {
		fmt.Println("New filename: ", fileName)
	}
	return fileName
}

// Get the list of files to process in the target folder
func getFilesToProcess() []string {
	fileList := []string{}
	singleFileMode = false

	if dirName == "" {
		if inFileName != "" {
			// no Dir name provided, but file name provided =>
			// Single file mode
			singleFileMode = true
			fileList = append(fileList, inFileName)
			return fileList
		} else {
			// no Dir name, no file name
			fmt.Println("Input file name or working directory is not provided")
			usage()
		}
	}

	// We have working directory - takes over single file name, if both provided
	err := filepath.Walk(dirName, func(path string, f os.FileInfo, _ error) error {
		if isRawFile(path) {
			fileList = append(fileList, path)
			if diagnostics {
				fmt.Println("Added: ", path)
			}
		}
		return nil
	})

	if err != nil {
		fmt.Println("Error getting files list: ", err)
		os.Exit(-1)
	}

	sort.Strings(fileList)
	if diagnostics {
		for _, path := range fileList {
			fmt.Println(path)
		}
	}
	return fileList
}

func isRawFile(fileName string) bool {
	if diagnostics {
		fmt.Printf("Ext: %s\tVerifying file:%s\n", inExtension, fileName)
	}
	return filepath.Ext(fileName) == "."+inExtension
}
