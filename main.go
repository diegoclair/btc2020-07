package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	//import the Paho Go MQTT library
	"os"
	"strings"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var flag bool = false
var i int

type Data struct {
	Row          int    `json:"row" csv:"row" `
	Tempo        string `json:"Tempo" csv:"Tempo" `
	Estacao      string `json:"Estação" csv:"Estação" `
	LAT          string `json:"LAT" csv:"LAT" `
	LONG         string `json:"LONG" csv:"LONG" `
	Movimentacao string `json:"Movimentação" csv:"Movimentação" `
	Original473  string `json:"Original_473" csv:"Original_473" `
	Original269  string `json:"Original_269" csv:"Original_269" `
	Zero         string `json:"Zero" csv:"Zero" `
	MacaVerde    string `json:"Maçã-Verde" csv:"Maçã-Verde" `
	Tangerina    string `json:"Tangerina" csv:"Tangerina" `
	Citrus       string `json:"Citrus" csv:"Citrus" `
	AcaiGuarana  string `json:"Açaí-Guaraná" csv:"Açaí-Guaraná" `
	Pessego      string `json:"Pêssego" csv:"Pêssego" `
	TARGET       string `json:"TARGET" csv:"TARGET" `
}

// ToCSVRow Return struct in a csv row format
func (d Data) ToCSVRow() []string {
	return []string{
		strconv.Itoa(d.Row),
		d.Tempo,
		d.Estacao,
		d.LAT,
		d.LONG,
		d.Movimentacao,
		d.Original473,
		d.Original269,
		d.Zero,
		d.MacaVerde,
		d.Tangerina,
		d.Citrus,
		d.AcaiGuarana,
		d.Pessego,
		d.TARGET,
	}
}

//ToCSVHeader returns the headers of csv based on tag "csv" on struct fields
func (d Data) ToCSVHeader() []string {
	t := reflect.TypeOf(d)

	retVal := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("csv")
		retVal[i] = tag
	}
	return retVal
}

//define a function for the default message handler
var f MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	payload := msg.Payload()
	if strings.Compare(string(payload), "\n") > 0 {

		var data Data

		err := json.Unmarshal([]byte(payload), &data)
		if err != nil {
			flag = true
		}

		err = writeCsvFile(data)
		if err != nil {
			flag = true
		}

		fmt.Printf("MSG: %s\n", payload)

		if i == 3 {
			flag = true
		}
	}

	if strings.Compare("bye\n", string(payload)) == 0 {
		fmt.Println("exitting")
		flag = true
	}
}

func main() {
	//create a ClientOptions struct setting the broker address, clientid, turn
	//off trace output and set the default message handler
	opts := MQTT.NewClientOptions().AddBroker("tcp://tnt-iot.maratona.dev:30573")
	opts.SetClientID("Device-sub")
	opts.SetDefaultPublishHandler(f)
	opts.SetUsername("maratoners")
	opts.SetPassword("ndsjknvkdnvjsbvj")

	//create and start a client using the above ClientOptions
	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	//subscribe to the topic /go-mqtt/sample and request messages to be delivered
	//at a maximum qos of zero, wait for the receipt to confirm the subscription
	if token := c.Subscribe("tnt", 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for flag == false {
		time.Sleep(1 * time.Second)
	}

	//unsubscribe from /go-mqtt/sample
	if token := c.Unsubscribe("tnt"); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	c.Disconnect(250)
}

func writeCsvFile(data Data) error {

	path := "./dataset.csv"
	fileCSV, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil && strings.Contains(err.Error(), "no such file or directory") {
		fileCSV, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
		if err != nil {
			fmt.Println("Error to create file: ", err)
			return err
		}
	} else if err != nil {
		fmt.Println("Error to open file: ", err)
		return err
	}

	w := bufio.NewWriterSize(fileCSV, 4096*2)
	wr := csv.NewWriter(w)

	fileInfo, err := fileCSV.Stat()
	if err != nil {
		return err
	}

	if i == 0 && fileInfo.Size() == 0 {
		wr.Write(data.ToCSVHeader())
	}
	i++

	wr.Write(data.ToCSVRow())
	wr.Flush()

	_, err = fileCSV.Seek(0, 0)
	if err != nil {
		return err
	}

	fileCSV.Close()

	return nil
}
