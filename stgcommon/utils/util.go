package utils

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

func identifyPanic() string {
	var name, file string
	var line int
	var pc [16]uintptr

	n := runtime.Callers(3, pc[:])
	for _, pc := range pc[:n] {
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}
		file, line = fn.FileLine(pc)
		name = fn.Name()
		if !strings.HasPrefix(name, "runtime.") {
			break
		}
	}

	switch {
	case name != "":
		return fmt.Sprintf("%v:%v", name, line)
	case file != "":
		return fmt.Sprintf("%v:%v", file, line)
	}

	return fmt.Sprintf("pc:%x", pc)
}

// RecoveredFn panic recover func
func RecoveredFn(cbs ...func()) {
	if err := recover(); err != nil {
		log.Printf("Recovered in %v: %s\n", err, string(debug.Stack()))
		log.Println(identifyPanic())
		for _, cb := range cbs {
			cb()
		}
	}
}

func TimeMillisecondToHumanString(t time.Time) string {
	return fmt.Sprintf("%04d%02d%02d%02d%02d%02d%03d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
}
