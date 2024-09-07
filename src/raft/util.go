package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Debugging
/* const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
} */
type logTopic string
const (
	dLog logTopic = "LOG" // AppendEntries
	dVote logTopic = "VOTE" // RequestVote
	dElection logTopic = "ELCT"
	dHeartbeat logTopic = "HRBT"
	dClient logTopic = "CLNT"
	dCommit logTopic = "CMIT"
	dDrop logTopic = "DROP"
	dLeader logTopic = "LEAD"
	dPersist logTopic = "PERS"
	dSnap logTopic = "SNAP"
	dTerm logTopic = "TERM"

	dTrace logTopic = "TRACE"
	dInfo logTopic = "INFO"
	dWarn logTopic = "WARN"
	dError logTopic = "ERROR"
	dFatal logTopic = "FATAL"
)

var debugStart time.Time
var debugVerbosity int

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid vervosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

const Padding = "      "

func (rf *Raft) Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity > 0 {
		log.Print(rf.Sdebug(topic, format, a...))
	}
	return
}

func (rf *Raft) Sdebug(topic logTopic, format string, a ...interface{}) string {
	preamble := strings.Repeat(Padding, rf.me)
	epilogue := strings.Repeat(Padding, len(rf.peers) - rf.me - 1)
	prefix := fmt.Sprintf("%s%s %-5s [%s t%02d S%d] %s", preamble, Microseconds(time.Now()), string(topic), rf.state, rf.term, rf.me, epilogue)
	format = prefix + format
	return fmt.Sprintf(format, a...)
}

func Microseconds(t time.Time) string {
	return fmt.Sprintf("%06d", t.Sub(debugStart).Microseconds() / 100)
}

