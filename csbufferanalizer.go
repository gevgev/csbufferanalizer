package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	inFileName     string
	dirName        string
	inExtension    string
	diagnostics    bool
	outputFormat   string
	outputFileName string
	concurrency    int
	verbose        bool
	singleFileMode bool
	appName        string
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
		appName = os.Args[0]
		if inFileName == "" && dirName == "" && len(os.Args) == 2 {
			inFileName = os.Args[1]
		}
	} else {
		usage()
	}

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

// just extract timestamp, device Id, and calculate event size
func parseEvent(line string) (timestamp time.Time, deviceId string, eventSize int, err error) {
	defer func() {
		if r := recover(); r != nil {
			timestamp = time.Now()
			err = errors.New("Parser time exception")
		}
	}()

	tokens := strings.Split(line, " ")
	if len(tokens) != 2 {
		return time.Now(), "", 0, errors.New("Wrong line format")
	}

	deviceId = tokens[0]
	clickString := tokens[1]
	timestamp = convertToTime(clickString[2:10])
	eventSize = len(clickString) / 2
	err = nil

	if timestamp.After(time.Now()) {
		err = errors.New("Wrong date: " + timestamp.String())
	}
	return
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
}

func (pkg Package) String() string {
	return fmt.Sprintf("%v, %s", pkg.timestamp, pkg.deviceId)
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
func Pack(timestamp time.Time, deviceId string) Package {
	pkg := Package{}

	pkg.deviceId = deviceId
	pkg.timestamp = timestamp

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
		fmt.Fprintln(w, pkg)
	}
	w.Flush()
	file.Close()
}

func main() {
	startTime := time.Now()
	rand.Seed(int64(startTime.Second()))

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

		scanner := bufio.NewScanner(file)
		lineNo := 0
		for scanner.Scan() {
			line := scanner.Text()
			lineNo++
			timestamp, deviceId, eventSize, err := parseEvent(line)
			if err != nil {
				logErrorEvent(fileName, line, lineNo, err)
			} else {
				if _, ok := bufferSize[deviceId]; !ok {
					// First occurence
					bufferSize[deviceId] = rand.Intn(BuffWaterMarkSize)
				}
				if bufferSize[deviceId]+eventSize > BuffWaterMarkSize {
					pkg := Pack(timestamp, deviceId)
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
		totalEvents += lineNo
		file.Close()
	}

	printOutputFile(packages)
	max, avg, total := printEventsPerSecond(packages)
	printErrorLogs()
	fmt.Println("Number of devices:\t", len(bufferSize))
	fmt.Println("Total events: \t\t", totalEvents)
	fmt.Println("Total packages:\t\t", len(packages))
	fmt.Println("First package sent at: ", packages[0].timestamp)
	fmt.Println("Last  package sent at: ", packages[len(packages)-1].timestamp)
	fmt.Println("Error entries number: ", len(errorsLog))
	fmt.Println("Total reported at times: ", total)
	fmt.Printf("Max per second: %d at %v\n", max.numberOfEvents, max.timestamp)
	fmt.Println("Average per second: ", avg)
	fmt.Printf("Processed %d files in %v\n", len(files), time.Since(startTime))
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
		if _, ok := eventsPerSecond[pkg.timestamp]; ok {
			eventsPerSecond[pkg.timestamp]++
		} else {
			eventsPerSecond[pkg.timestamp] = 1
		}
	}

	var orderedEventsPerSecond TimepointTypeList
	for k, v := range eventsPerSecond {
		orderedEventsPerSecond = append(orderedEventsPerSecond, TimepointType{k, v})
	}

	sort.Sort(orderedEventsPerSecond)

	file, err := os.Create("eventsPerSecond.csv")
	if err != nil {
		fmt.Println(err)
	}
	w := bufio.NewWriter(file)
	for _, points := range orderedEventsPerSecond {
		fmt.Fprintf(w, "%v, %d\n", points.timestamp, points.numberOfEvents)
		if points.numberOfEvents > max.numberOfEvents {
			max = points
		}

		avg += max.numberOfEvents
	}
	w.Flush()
	file.Close()

	avg = avg / len(orderedEventsPerSecond)
	total = len(orderedEventsPerSecond)

	return
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
	return fileList
}

func isRawFile(fileName string) bool {
	if diagnostics {
		fmt.Printf("Ext: %s\tVerifying file:%s\n", inExtension, fileName)
	}
	return filepath.Ext(fileName) == "."+inExtension
}
