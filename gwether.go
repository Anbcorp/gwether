package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	// TODO: Get rid of this one ?
	"github.com/sevlyar/go-daemon"
	"github.com/ziutek/rrd"
)

type Event struct {
	Time  MqttTime `json:"time"`
	Model string   `json:"model"`
	// TODO: id maybe an hex string
	ID           int     `json:"id"`
	Channel      int     `json:"channel"`
	TemperatureC float64 `json:"temperature_C"`
}

var (
	updaters   = make(map[string]*rrd.Updater)
	updatersMu sync.RWMutex
	debug      bool
)

//var nonASCIIOrUpperRegexp = regexp.MustCompile(`[^a-z0-9]+`)

func sanitize(s string) string {
	s = strings.ToLower(s)
	// TODO: Sanitize properly for 'correct' ascii filenames
	//s = nonASCIIOrUpperRegexp.ReplaceAllString(s, "")
	if debug {
		log.Printf("Sanitized model: %s", s)
	}
	return s
}

func getUpdater(rrdFile string) *rrd.Updater {
	updatersMu.RLock()
	updater, exists := updaters[rrdFile]
	updatersMu.RUnlock()

	if exists {
		if debug {
			log.Printf("Reusing updater for %s", rrdFile)
		}
		return updater
	}

	updatersMu.Lock()
	defer updatersMu.Unlock()

	if debug {
		log.Printf("Creating new updater for %s", rrdFile)
	}
	updater = rrd.NewUpdater(rrdFile)
	updater.SetTemplate("temp_C")
	updaters[rrdFile] = updater
	return updater
}

func run(broker, rrdDir, username, password string) {
	if debug {
		log.Println("Connecting to MQTT broker...")
	}
	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID("rrd-writer")
	opts.SetUsername(username)
	opts.SetPassword(password)
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
	}
	if debug {
		log.Println("Connected to MQTT broker.")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if debug {
		log.Println("Subscribing to topic 'events'...")
	}
	if token := client.Subscribe("rtl_433/cubieboard/events", 0, func(client mqtt.Client, msg mqtt.Message) {
		if debug {
			log.Printf("Received message: %s", string(msg.Payload()))
		}

		var event Event
		if err := json.Unmarshal(msg.Payload(), &event); err != nil {
			log.Printf("Error parsing JSON: %v", err)
			return
		}

		model := sanitize(event.Model)
		rrdFileName := fmt.Sprintf("%s-%d-%d.rrd", model, event.ID, event.Channel)
		rrdFile := rrdDir + string(os.PathSeparator) + rrdFileName

		updater := getUpdater(rrdFile)
		if debug {
			log.Printf("Updating RRD file: %s with temperature: %f at time: %s", rrdFile, event.TemperatureC, event.Time)
		}
		err := updater.Update(event.Time.Unix(), event.TemperatureC)
		if err != nil {
			log.Printf("Error updating RRD: %v", err)
		}
	}); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to subscribe: %v", token.Error())
	}
	if debug {
		log.Println("Subscribed to topic 'events'.")
	}

	<-ctx.Done()
	if debug {
		log.Println("Shutting down gracefully...")
	}
	client.Disconnect(250)
}

func usage() {
	fmt.Fprintf(flag.CommandLine.Output(), `Usage: rrd-writer [options]

Options:
`)
	flag.PrintDefaults()
}

func main() {
	daemonize := flag.Bool("daemon", false, "Run as daemon")
	debugFlag := flag.Bool("debug", false, "Enable debug logging")
	broker := flag.String("broker", "tcp://localhost:1883", "MQTT broker address")
	rrdDir := flag.String("rrd", "", "rrd files directory")
	username := flag.String("username", "", "MQTT username")
	password := flag.String("password", "", "MQTT password")
	help := flag.Bool("help", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	debug = *debugFlag

	if *daemonize {
		cntxt := &daemon.Context{
			PidFileName: "rrd-writer.pid",
			PidFilePerm: 0644,
			LogFileName: "rrd-writer.log",
			LogFilePerm: 0640,
			WorkDir:     "./",
			Umask:       027,
		}

		d, err := cntxt.Reborn()
		if err != nil {
			log.Fatalf("Failed to daemonize: %v", err)
		}
		if d != nil {
			return
		}
		defer cntxt.Release()
	}

	run(*broker, *rrdDir, *username, *password)
}
