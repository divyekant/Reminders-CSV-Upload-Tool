package commands

import (
	"strings"
	"sync"

	"time"

	"strconv"

	"log"

	"encoding/csv"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

const (
	reminderUploadEndpoint = "in1-paytm.api.clevertap.com/1/upload"
)

type uploadRemindersFromCSVCommand struct {
}

func (u *uploadRemindersFromCSVCommand) Execute() {
	log.Println("started")

	done := make(chan interface{})

	var wg sync.WaitGroup

	if *globals.CSVFilePath != "" {
		batchAndSendToCTAPI(done, processReminderCSVLineForUpload(done, csvLineGenerator(done)), &wg)
	}

	if *globals.JSONFilePath != "" {
		batchAndSendToCTAPI(done, jsonLineGenerator(done), &wg)
	}

	wg.Wait()

	log.Println("done")

	log.Println("---------------------Summary---------------------")
	if *globals.Type == "profile" {
		log.Printf("Profiles Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
	} else {
		log.Printf("Events Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
	}
}

//identity, objectID, FBID or GPID

var headerRemindersKeys []string
var keysRemindersLen int
var tsRemindersExists = false

func isReminderId(val string) bool {
	if val == "reminderId" {
		return true
	}
	return false
}

func isReminderTs(val string) bool {
	if val == "reminderTs" {
		return true
	}
	return false
}

func processRemindersHeader(keys []string) bool {
	keys = cleanKeys(keys)
	identityExists := false
	reminderIdExists := false
	reminderTsExists := false

	for _, val := range keys {
		if isIdentity(val) {
			identityExists = true
		}
		if isReminderId(val) {
			reminderIdExists = true
		}
		if isReminderTs(val) {
			reminderTsExists = true
		}
	}
	if !identityExists {
		log.Println("identity, objectID, FBID or GPID should be present")
		return false
	}
	if !reminderIdExists {
		log.Println("Reminder ID not found")
		return false
	}
	if !reminderTsExists {
		log.Println("Reminder TS not found")
		return false
	}

	keysLen = len(keys)
	headerKeys = keys
	return true
}

func processReminderCSVUploadLine(vals []string, line string) (interface{}, bool) {
	rowLen := len(vals)
	if rowLen != keysLen {
		log.Println("Mismatch in header and row data length")
		return nil, false
	}
	record := make(map[string]interface{})

	record["type"] = "profile"
	reminderData := make(map[string]interface{})
	propertyData := make(map[string]interface{})
	profileData := make(map[string]interface{})

	for index, ep := range vals {
		key := headerKeys[index]
		if isIdentity(key) {
			if ep == "" {
				log.Println("Identity field is missing.")
				return nil, false
			}
			record[key] = ep
			continue
		}

		if isReminderTs(key) {
			if ep == "" {
				log.Println("Reminder TS is missing.")
				return nil, false
			}
			v, err := strconv.ParseInt(ep, 10, 64)
			if err == nil {
				reminderData[key] = v
			} else {
				log.Println("Reminder TS is not in Int format.")
				return nil, false
			}
			continue
		}

		if isReminderId(key) {
			if ep == "" {
				log.Println("Reminder ID is missing.")
				return nil, false
			}
			reminderData[key] = ep
			continue
		}

		reminderData["reminderType"] = globals.ReminderType
		reminderData["snooze"] = 0

		if globals.Schema != nil {
			dataType, ok := globals.Schema[key]
			if ok {
				dataTypeLower := strings.ToLower(dataType)
				if strings.HasPrefix(dataTypeLower, "date") {
					split := strings.Split(dataType, "$")
					t, err := time.Parse(split[1], ep+" "+split[2])
					if err == nil {
						epoch := t.Unix()
						propertyData[key] = "$D_" + strconv.FormatInt(epoch, 10)
					}
				}
				dataType = strings.ToLower(dataType)
				if dataType == "float" {
					v, err := strconv.ParseFloat(ep, 64)
					if err == nil {
						propertyData[key] = v
					}
				}
				if dataType == "integer" {
					v, err := strconv.ParseInt(ep, 10, 64)
					if err == nil {
						propertyData[key] = v
					}
				}
				if dataType == "boolean" {
					v, err := strconv.ParseBool(strings.ToLower(ep))
					if err == nil {
						propertyData[key] = v
					}
				}
				if *globals.Type == "profile" {
					if dataType == "string[]" {

						addArray := make(map[string][]string)
						result := strings.Split(ep, ",")
						for i := range result {
							addArray["$add"] = append(addArray["$add"], strings.TrimSpace(result[i]))
						}

						if len(addArray["$add"]) > 0 {
							propertyData[key] = addArray
						}

					}
				}
			}
		}
		_, ok := propertyData[key]
		if !ok {
			propertyData[key] = ep
		}
	}
	reminderData["props"] = propertyData
	x := []map[string]interface{}{}
	x = append(x, reminderData)
	profileData["reminders"] = x
	record["profileData"] = profileData

	return record, true
}

func processReminderCSVLineForUpload(done chan interface{}, rowStream <-chan csvLineInfo) <-chan interface{} {
	recordStream := make(chan interface{})
	go func() {
		defer close(recordStream)
		for lineInfo := range rowStream {
			i := lineInfo.LineNum
			l := lineInfo.Line
			//sLine := strings.Split(l, ",")
			r := csv.NewReader(strings.NewReader(l))
			sLineArr, err := r.ReadAll()
			if err != nil || len(sLineArr) != 1 {
				if i == 0 {
					log.Println("Error in processing header")
					select {
					case <-done:
						return
					default:
						done <- struct{}{}
						log.Println("...Exiting...")
						return
					}
				}
				if l != "" {
					log.Printf("Error in processing record")
					log.Printf("Skipping line number: %v : %v", i+1, l)
				}
				continue
			}
			sLine := sLineArr[0]
			if i == 0 {
				//header: line just process to get keys
				if !processRemindersHeader(sLine) {
					select {
					case <-done:
						return
					default:
						done <- struct{}{}
						log.Println("...Exiting...")
						return
					}
				}
			} else {
				record, shouldAdd := processReminderCSVUploadLine(sLine, l)
				if shouldAdd {
					select {
					case <-done:
						return
					case recordStream <- record:
					}
				} else {
					log.Println("Skipping line number: ", i+1, " : ", l)
				}
			}
		}
	}()
	return recordStream
}
