package main

import (
	"strings"
	"time"
)

type MqttTime struct {
	time.Time
}

const expiryDateLayout = "2006-01-02 15:04:05"

var tZ = time.Now().Location()

func (ct *MqttTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		ct.Time = time.Time{}
		return
	}
	ct.Time, err = time.ParseInLocation(expiryDateLayout, s, tZ)
	return
}
