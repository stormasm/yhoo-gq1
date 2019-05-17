/*
Package quote is free quote downloader library and cli

Downloads daily/weekly/monthly historical price quotes from Yahoo
and daily/intraday data from Google/Tiingo/Bittrex/Binance

Copyright 2017 Mark Chenoweth
Licensed under terms of MIT license (see LICENSE)
*/
package quote

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/textproto"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Quote - stucture for historical price data
type Quote struct {
	Symbol    string      `json:"symbol"`
	Precision int64       `json:"-"`
	Date      []time.Time `json:"date"`
	Open      []float64   `json:"open"`
	High      []float64   `json:"high"`
	Low       []float64   `json:"low"`
	Close     []float64   `json:"close"`
	Volume    []float64   `json:"volume"`
}

// Quotes - an array of historical price data
type Quotes []Quote

// Period - for quote history
type Period string

// ClientTimeout - connect/read timeout for client requests
const ClientTimeout = 10 * time.Second

const (
	// Min1 - 1 Minute time period
	Min1 Period = "60"
	// Min3 - 3 Minute time period
	Min3 Period = "3m"
	// Min5 - 5 Minute time period
	Min5 Period = "300"
	// Min15 - 15 Minute time period
	Min15 Period = "900"
	// Min30 - 30 Minute time period
	Min30 Period = "1800"
	// Min60 - 60 Minute time period
	Min60 Period = "3600"
	// Hour2 - 2 hour time period
	Hour2 Period = "2h"
	// Hour4 - 4 hour time period
	Hour4 Period = "4h"
	// Hour6 - 6 hour time period
	Hour6 Period = "6h"
	// Hour8 - 8 hour time period
	Hour8 Period = "8h"
	// Hour12 - 12 hour time period
	Hour12 Period = "12h"
	// Daily time period
	Daily Period = "d"
	// Day3 - 3 day time period
	Day3 Period = "3d"
	// Weekly time period
	Weekly Period = "w"
	// Monthly time period
	Monthly Period = "m"
)

// Log - standard logger, disabled by default
var Log *log.Logger

// Delay - time delay in milliseconds between quote requests (default=100)
// Be nice, don't get blocked
var Delay time.Duration

func init() {
	Log = log.New(ioutil.Discard, "quote: ", log.Ldate|log.Ltime|log.Lshortfile)
	Delay = 100
}

// NewQuote - new empty Quote struct
func NewQuote(symbol string, bars int) Quote {
	return Quote{
		Symbol: symbol,
		Date:   make([]time.Time, bars),
		Open:   make([]float64, bars),
		High:   make([]float64, bars),
		Low:    make([]float64, bars),
		Close:  make([]float64, bars),
		Volume: make([]float64, bars),
	}
}

// ParseDateString - parse a potentially partial date string to Time
func ParseDateString(dt string) time.Time {
	if dt == "" {
		return time.Now()
	}
	t, _ := time.Parse("2006-01-02 15:04", dt+"0000-01-01 00:00"[len(dt):])
	return t
}

func getPrecision(symbol string) int {
	var precision int
	precision = 2
	if strings.Contains(strings.ToUpper(symbol), "BTC") ||
		strings.Contains(strings.ToUpper(symbol), "ETH") ||
		strings.Contains(strings.ToUpper(symbol), "USD") {
		precision = 8
	}
	return precision
}

// CSV - convert Quote structure to csv string
func (q Quote) CSV() string {

	precision := getPrecision(q.Symbol)

	var buffer bytes.Buffer
	buffer.WriteString("datetime,open,high,low,close,volume\n")
	for bar := range q.Close {
		str := fmt.Sprintf("%s,%.*f,%.*f,%.*f,%.*f,%.*f\n", q.Date[bar].Format("2006-01-02 15:04"),
			precision, q.Open[bar], precision, q.High[bar], precision, q.Low[bar], precision, q.Close[bar], precision, q.Volume[bar])
		buffer.WriteString(str)
	}
	return buffer.String()
}

// Highstock - convert Quote structure to Highstock json format
func (q Quote) Highstock() string {

	precision := getPrecision(q.Symbol)

	var buffer bytes.Buffer
	buffer.WriteString("[\n")
	for bar := range q.Close {
		comma := ","
		if bar == len(q.Close)-1 {
			comma = ""
		}
		str := fmt.Sprintf("[%d,%.*f,%.*f,%.*f,%.*f,%.*f]%s\n",
			q.Date[bar].UnixNano()/1000000, precision, q.Open[bar], precision, q.High[bar], precision, q.Low[bar], precision, q.Close[bar], precision, q.Volume[bar], comma)
		buffer.WriteString(str)

	}
	buffer.WriteString("]\n")
	return buffer.String()
}

// Amibroker - convert Quote structure to csv string
func (q Quote) Amibroker() string {

	precision := getPrecision(q.Symbol)

	var buffer bytes.Buffer
	buffer.WriteString("date,time,open,high,low,close,volume\n")
	for bar := range q.Close {
		str := fmt.Sprintf("%s,%s,%.*f,%.*f,%.*f,%.*f,%.*f\n", q.Date[bar].Format("2006-01-02"), q.Date[bar].Format("15:04"),
			precision, q.Open[bar], precision, q.High[bar], precision, q.Low[bar], precision, q.Close[bar], precision, q.Volume[bar])
		buffer.WriteString(str)
	}
	return buffer.String()
}

// WriteCSV - write Quote struct to csv file
func (q Quote) WriteCSV(filename string) error {
	if filename == "" {
		if q.Symbol != "" {
			filename = q.Symbol + ".csv"
		} else {
			filename = "quote.csv"
		}
	}
	csv := q.CSV()
	return ioutil.WriteFile(filename, []byte(csv), 0644)
}

// WriteAmibroker - write Quote struct to csv file
func (q Quote) WriteAmibroker(filename string) error {
	if filename == "" {
		if q.Symbol != "" {
			filename = q.Symbol + ".csv"
		} else {
			filename = "quote.csv"
		}
	}
	csv := q.Amibroker()
	return ioutil.WriteFile(filename, []byte(csv), 0644)
}

// WriteHighstock - write Quote struct to Highstock json format
func (q Quote) WriteHighstock(filename string) error {
	if filename == "" {
		if q.Symbol != "" {
			filename = q.Symbol + ".json"
		} else {
			filename = "quote.json"
		}
	}
	csv := q.Highstock()
	return ioutil.WriteFile(filename, []byte(csv), 0644)
}

// NewQuoteFromCSV - parse csv quote string into Quote structure
func NewQuoteFromCSV(symbol, csv string) (Quote, error) {

	tmp := strings.Split(csv, "\n")
	numrows := len(tmp) - 1
	q := NewQuote("", numrows-1)

	for row, bar := 1, 0; row < numrows; row, bar = row+1, bar+1 {
		line := strings.Split(tmp[row], ",")
		q.Date[bar], _ = time.Parse("2006-01-02 15:04", line[0])
		q.Open[bar], _ = strconv.ParseFloat(line[1], 64)
		q.High[bar], _ = strconv.ParseFloat(line[2], 64)
		q.Low[bar], _ = strconv.ParseFloat(line[3], 64)
		q.Close[bar], _ = strconv.ParseFloat(line[4], 64)
		q.Volume[bar], _ = strconv.ParseFloat(line[5], 64)
	}
	return q, nil
}

// NewQuoteFromCSV - parse csv quote string into Quote structure
// with specified DateTime format
func NewQuoteFromCSVDateFormat(symbol, csv string, format string) (Quote, error) {

	tmp := strings.Split(csv, "\n")
	numrows := len(tmp) - 1
	q := NewQuote("", numrows-1)

	if len(strings.TrimSpace(format)) == 0 {
		format = "2006-01-02 15:04"
	}

	for row, bar := 1, 0; row < numrows; row, bar = row+1, bar+1 {
		line := strings.Split(tmp[row], ",")
		q.Date[bar], _ = time.Parse(format, line[0])
		q.Open[bar], _ = strconv.ParseFloat(line[1], 64)
		q.High[bar], _ = strconv.ParseFloat(line[2], 64)
		q.Low[bar], _ = strconv.ParseFloat(line[3], 64)
		q.Close[bar], _ = strconv.ParseFloat(line[4], 64)
		q.Volume[bar], _ = strconv.ParseFloat(line[5], 64)
	}
	return q, nil
}

// NewQuoteFromCSVFile - parse csv quote file into Quote structure
func NewQuoteFromCSVFile(symbol, filename string) (Quote, error) {
	csv, err := ioutil.ReadFile(filename)
	if err != nil {
		return NewQuote("", 0), err
	}
	return NewQuoteFromCSV(symbol, string(csv))
}

// NewQuoteFromCSVFile - parse csv quote file into Quote structure
// with specified DateTime format
func NewQuoteFromCSVFileDateFormat(symbol, filename string, format string) (Quote, error) {
	csv, err := ioutil.ReadFile(filename)
	if err != nil {
		return NewQuote("", 0), err
	}
	return NewQuoteFromCSVDateFormat(symbol, string(csv), format)
}

// JSON - convert Quote struct to json string
func (q Quote) JSON(indent bool) string {
	var j []byte
	if indent {
		j, _ = json.MarshalIndent(q, "", "  ")
	} else {
		j, _ = json.Marshal(q)
	}
	return string(j)
}

// WriteJSON - write Quote struct to json file
func (q Quote) WriteJSON(filename string, indent bool) error {
	if filename == "" {
		filename = q.Symbol + ".json"
	}
	json := q.JSON(indent)
	return ioutil.WriteFile(filename, []byte(json), 0644)

}

// NewQuoteFromJSON - parse json quote string into Quote structure
func NewQuoteFromJSON(jsn string) (Quote, error) {
	q := Quote{}
	err := json.Unmarshal([]byte(jsn), &q)
	if err != nil {
		return q, err
	}
	return q, nil
}

// NewQuoteFromJSONFile - parse json quote string into Quote structure
func NewQuoteFromJSONFile(filename string) (Quote, error) {
	jsn, err := ioutil.ReadFile(filename)
	if err != nil {
		return NewQuote("", 0), err
	}
	return NewQuoteFromJSON(string(jsn))
}

// CSV - convert Quotes structure to csv string
func (q Quotes) CSV() string {

	var buffer bytes.Buffer

	buffer.WriteString("symbol,datetime,open,high,low,close,volume\n")

	for sym := 0; sym < len(q); sym++ {
		quote := q[sym]
		precision := getPrecision(quote.Symbol)
		for bar := range quote.Close {
			str := fmt.Sprintf("%s,%s,%.*f,%.*f,%.*f,%.*f,%.*f\n",
				quote.Symbol, quote.Date[bar].Format("2006-01-02 15:04"), precision, quote.Open[bar], precision, quote.High[bar], precision, quote.Low[bar], precision, quote.Close[bar], precision, quote.Volume[bar])
			buffer.WriteString(str)
		}
	}

	return buffer.String()
}

// Highstock - convert Quotes structure to Highstock json format
func (q Quotes) Highstock() string {

	var buffer bytes.Buffer

	buffer.WriteString("{")

	for sym := 0; sym < len(q); sym++ {
		quote := q[sym]
		precision := getPrecision(quote.Symbol)
		for bar := range quote.Close {
			comma := ","
			if bar == len(quote.Close)-1 {
				comma = ""
			}
			if bar == 0 {
				buffer.WriteString(fmt.Sprintf("\"%s\":[\n", quote.Symbol))
			}
			str := fmt.Sprintf("[%d,%.*f,%.*f,%.*f,%.*f,%.*f]%s\n",
				quote.Date[bar].UnixNano()/1000000, precision, quote.Open[bar], precision, quote.High[bar], precision, quote.Low[bar], precision, quote.Close[bar], precision, quote.Volume[bar], comma)
			buffer.WriteString(str)
		}
		if sym < len(q)-1 {
			buffer.WriteString("],\n")
		} else {
			buffer.WriteString("]\n")
		}
	}

	buffer.WriteString("}")

	return buffer.String()
}

// Amibroker - convert Quotes structure to csv string
func (q Quotes) Amibroker() string {

	var buffer bytes.Buffer

	buffer.WriteString("symbol,date,time,open,high,low,close,volume\n")

	for sym := 0; sym < len(q); sym++ {
		quote := q[sym]
		precision := getPrecision(quote.Symbol)
		for bar := range quote.Close {
			str := fmt.Sprintf("%s,%s,%s,%.*f,%.*f,%.*f,%.*f,%.*f\n",
				quote.Symbol, quote.Date[bar].Format("2006-01-02"), quote.Date[bar].Format("15:04"), precision, quote.Open[bar], precision, quote.High[bar], precision, quote.Low[bar], precision, quote.Close[bar], precision, quote.Volume[bar])
			buffer.WriteString(str)
		}
	}

	return buffer.String()
}

// WriteCSV - write Quotes structure to file
func (q Quotes) WriteCSV(filename string) error {
	if filename == "" {
		filename = "quotes.csv"
	}
	csv := q.CSV()
	ba := []byte(csv)
	return ioutil.WriteFile(filename, ba, 0644)
}

// WriteAmibroker - write Quotes structure to file
func (q Quotes) WriteAmibroker(filename string) error {
	if filename == "" {
		filename = "quotes.csv"
	}
	csv := q.Amibroker()
	ba := []byte(csv)
	return ioutil.WriteFile(filename, ba, 0644)
}

// NewQuotesFromCSV - parse csv quote string into Quotes array
func NewQuotesFromCSV(csv string) (Quotes, error) {

	quotes := Quotes{}
	tmp := strings.Split(csv, "\n")
	numrows := len(tmp) - 1

	var index = make(map[string]int)
	for idx := 1; idx < numrows; idx++ {
		sym := strings.Split(tmp[idx], ",")[0]
		index[sym]++
	}

	row := 1
	for sym, len := range index {
		q := NewQuote(sym, len)
		for bar := 0; bar < len; bar++ {
			line := strings.Split(tmp[row], ",")
			q.Date[bar], _ = time.Parse("2006-01-02 15:04", line[1])
			q.Open[bar], _ = strconv.ParseFloat(line[2], 64)
			q.High[bar], _ = strconv.ParseFloat(line[3], 64)
			q.Low[bar], _ = strconv.ParseFloat(line[4], 64)
			q.Close[bar], _ = strconv.ParseFloat(line[5], 64)
			q.Volume[bar], _ = strconv.ParseFloat(line[6], 64)
			row++
		}
		quotes = append(quotes, q)
	}
	return quotes, nil
}

// NewQuotesFromCSVFile - parse csv quote file into Quotes array
func NewQuotesFromCSVFile(filename string) (Quotes, error) {
	csv, err := ioutil.ReadFile(filename)
	if err != nil {
		return Quotes{}, err
	}
	return NewQuotesFromCSV(string(csv))
}

// JSON - convert Quotes struct to json string
func (q Quotes) JSON(indent bool) string {
	var j []byte
	if indent {
		j, _ = json.MarshalIndent(q, "", "  ")
	} else {
		j, _ = json.Marshal(q)
	}
	return string(j)
}

// WriteJSON - write Quote struct to json file
func (q Quotes) WriteJSON(filename string, indent bool) error {
	if filename == "" {
		filename = "quotes.json"
	}
	jsn := q.JSON(indent)
	return ioutil.WriteFile(filename, []byte(jsn), 0644)
}

// WriteHighstock - write Quote struct to json file in Highstock format
func (q Quotes) WriteHighstock(filename string) error {
	if filename == "" {
		filename = "quotes.json"
	}
	hc := q.Highstock()
	return ioutil.WriteFile(filename, []byte(hc), 0644)
}

// NewQuotesFromJSON - parse json quote string into Quote structure
func NewQuotesFromJSON(jsn string) (Quotes, error) {
	quotes := Quotes{}
	err := json.Unmarshal([]byte(jsn), &quotes)
	if err != nil {
		return quotes, err
	}
	return quotes, nil
}

// NewQuotesFromJSONFile - parse json quote string into Quote structure
func NewQuotesFromJSONFile(filename string) (Quotes, error) {
	jsn, err := ioutil.ReadFile(filename)
	if err != nil {
		return Quotes{}, err
	}
	return NewQuotesFromJSON(string(jsn))
}

// NewQuoteFromYahoo - Yahoo historical prices for a symbol
func NewQuoteFromYahoo(symbol, startDate, endDate string, period Period, adjustQuote bool) (Quote, error) {

	if period != Daily {
		Log.Printf("Yahoo intraday data no longer supported\n")
		return NewQuote("", 0), errors.New("Yahoo intraday data no longer supported")
	}

	from := ParseDateString(startDate)
	to := ParseDateString(endDate)

	// Get crumb
	jar, _ := cookiejar.New(nil)
	client := &http.Client{
		Timeout: ClientTimeout,
		Jar:     jar,
	}

	initReq, err := http.NewRequest("GET", "https://finance.yahoo.com", nil)
	if err != nil {
		return NewQuote("", 0), err
	}
	initReq.Header.Set("User-Agent", "Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11")
	resp, _ := client.Do(initReq)

	crumbReq, err := http.NewRequest("GET", "https://query1.finance.yahoo.com/v1/test/getcrumb", nil)
	if err != nil {
		return NewQuote("", 0), err
	}
	crumbReq.Header.Set("User-Agent", "Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11")
	resp, _ = client.Do(crumbReq)

	reader := csv.NewReader(resp.Body)
	crumb, err := reader.Read()
	if err != nil {
		Log.Printf("error getting crumb for '%s'\n", symbol)
		return NewQuote("", 0), err
	}

	url := fmt.Sprintf(
		"https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%d&period2=%d&interval=1d&events=history&crumb=%s",
		symbol,
		from.Unix(),
		to.Unix(),
		crumb[0])
	resp, err = client.Get(url)
	if err != nil {
		Log.Printf("symbol '%s' not found\n", symbol)
		return NewQuote("", 0), err
	}
	defer resp.Body.Close()

	var csvdata [][]string
	reader = csv.NewReader(resp.Body)
	csvdata, err = reader.ReadAll()
	if err != nil {
		Log.Printf("bad data for symbol '%s'\n", symbol)
		return NewQuote("", 0), err
	}

	numrows := len(csvdata) - 1
	quote := NewQuote(symbol, numrows)

	for row := 1; row < len(csvdata); row++ {

		// Parse row of data
		d, _ := time.Parse("2006-01-02", csvdata[row][0])
		o, _ := strconv.ParseFloat(csvdata[row][1], 64)
		h, _ := strconv.ParseFloat(csvdata[row][2], 64)
		l, _ := strconv.ParseFloat(csvdata[row][3], 64)
		c, _ := strconv.ParseFloat(csvdata[row][4], 64)
		a, _ := strconv.ParseFloat(csvdata[row][5], 64)
		v, _ := strconv.ParseFloat(csvdata[row][6], 64)

		quote.Date[row-1] = d

		// Adjustment ratio
		if adjustQuote {
			quote.Open[row-1] = o
			quote.High[row-1] = h
			quote.Low[row-1] = l
			quote.Close[row-1] = a
		} else {
			ratio := c / a
			quote.Open[row-1] = o * ratio
			quote.High[row-1] = h * ratio
			quote.Low[row-1] = l * ratio
			quote.Close[row-1] = c
		}

		quote.Volume[row-1] = v

	}

	return quote, nil
}

// NewQuotesFromYahoo - create a list of prices from symbols in file
func NewQuotesFromYahoo(filename, startDate, endDate string, period Period, adjustQuote bool) (Quotes, error) {

	quotes := Quotes{}
	inFile, err := os.Open(filename)
	if err != nil {
		return quotes, err
	}
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		sym := scanner.Text()
		quote, err := NewQuoteFromYahoo(sym, startDate, endDate, period, adjustQuote)
		if err == nil {
			quotes = append(quotes, quote)
		}
		time.Sleep(Delay * time.Millisecond)
	}
	return quotes, nil
}

// NewQuotesFromYahooSyms - create a list of prices from symbols in string array
func NewQuotesFromYahooSyms(symbols []string, startDate, endDate string, period Period, adjustQuote bool) (Quotes, error) {

	quotes := Quotes{}
	for _, symbol := range symbols {
		quote, err := NewQuoteFromYahoo(symbol, startDate, endDate, period, adjustQuote)
		if err == nil {
			quotes = append(quotes, quote)
		}
		time.Sleep(Delay * time.Millisecond)
	}
	return quotes, nil
}

func googleDaily(symbol string, from, to time.Time) (Quote, error) {

	args := fmt.Sprintf(
		"http://finance.google.com/finance/historical?q=%s&startdate=%s&enddate=%s&output=csv",
		symbol,
		url.QueryEscape(from.Format("Jan 2, 2006")),
		url.QueryEscape(to.Format("Jan 2, 2006")))

	resp, err := http.Get(args)
	if err != nil {
		Log.Printf("symbol '%s' not found\n", symbol)
		return NewQuote("", 0), err
	}
	defer resp.Body.Close()

	contents, _ := ioutil.ReadAll(resp.Body)
	tmp := strings.Join(strings.Split(string(contents), "\n")[1:], "\n")
	reader := csv.NewReader(strings.NewReader(tmp))
	csvdata, err := reader.ReadAll()
	if err != nil {
		Log.Printf("bad data for symbol '%s'\n", symbol)
		return NewQuote("", 0), err
	}

	numrows := len(csvdata)
	quote := NewQuote(symbol, numrows)

	for row := 0; row < numrows; row++ {
		bar := numrows - 1 - row // reverse the order
		quote.Date[bar], _ = time.Parse("2-Jan-06", csvdata[row][0])
		quote.Open[bar], _ = strconv.ParseFloat(csvdata[row][1], 64)
		quote.High[bar], _ = strconv.ParseFloat(csvdata[row][2], 64)
		quote.Low[bar], _ = strconv.ParseFloat(csvdata[row][3], 64)
		quote.Close[bar], _ = strconv.ParseFloat(csvdata[row][4], 64)
		quote.Volume[bar], _ = strconv.ParseFloat(csvdata[row][5], 64)
	}

	return quote, nil
}

func googleIntra(symbol string, from, to time.Time, period Period) (Quote, error) {

	args := fmt.Sprintf(
		"http://finance.google.com/finance/getprices?q=%s&i=%s&p=60d&f=d,o,h,l,c,v",
		strings.ToUpper(symbol),
		period)

	resp, err := http.Get(args)
	if err != nil {
		Log.Printf("symbol '%s' not found\n", symbol)
		return NewQuote("", 0), err
	}
	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)

	// ignore timezone row
	tmp := strings.Split(string(contents), "\n")[7:]
	var lines []string
	for _, line := range tmp {
		if !strings.HasPrefix(line, "TIMEZONE") {
			lines = append(lines, line)
		}
	}
	numrows := len(lines) - 1
	quote := NewQuote(symbol, numrows)

	var day int64
	for row := 0; row < numrows; row++ {

		csvdata := strings.Split(lines[row], ",")
		var offset int64
		z := csvdata[0]

		if z[0] == 'a' {
			day, _ = strconv.ParseInt(z[1:], 10, 64)
			offset = 0
		} else {
			offset, _ = strconv.ParseInt(z, 10, 64)
		}

		seconds, _ := strconv.ParseInt(string(period), 10, 64)
		quote.Date[row] = time.Unix(day+(seconds*offset), 0)
		quote.Open[row], _ = strconv.ParseFloat(csvdata[4], 64)
		quote.High[row], _ = strconv.ParseFloat(csvdata[2], 64)
		quote.Low[row], _ = strconv.ParseFloat(csvdata[3], 64)
		quote.Close[row], _ = strconv.ParseFloat(csvdata[1], 64)
		quote.Volume[row], _ = strconv.ParseFloat(csvdata[5], 64)
	}
	return quote, nil
}

// NewQuoteFromGoogle - Google daily/intraday historical prices for a symbol
func NewQuoteFromGoogle(symbol, startDate, endDate string, period Period) (Quote, error) {

	from := ParseDateString(startDate)
	to := ParseDateString(endDate)

	if period == Daily {
		return googleDaily(symbol, from, to)
	}
	return googleIntra(symbol, from, to, period)
}

// NewQuotesFromGoogle - create a list of prices from symbols in file
func NewQuotesFromGoogle(filename, startDate, endDate string, period Period) (Quotes, error) {

	quotes := Quotes{}
	inFile, err := os.Open(filename)
	if err != nil {
		return quotes, err
	}
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		sym := scanner.Text()
		quote, err := NewQuoteFromGoogle(sym, startDate, endDate, period)
		if err == nil {
			quotes = append(quotes, quote)
		} else {
			Log.Println("error downloading " + sym)
		}
		time.Sleep(Delay * time.Millisecond)
	}
	return quotes, nil
}

// NewQuotesFromGoogleSyms - create a list of prices from symbols in string array
func NewQuotesFromGoogleSyms(symbols []string, startDate, endDate string, period Period) (Quotes, error) {

	quotes := Quotes{}
	for _, symbol := range symbols {
		quote, err := NewQuoteFromGoogle(symbol, startDate, endDate, period)
		if err == nil {
			quotes = append(quotes, quote)
		} else {
			Log.Println("error downloading " + symbol)
		}
		time.Sleep(Delay * time.Millisecond)
	}
	return quotes, nil
}

// NewEtfList - download a list of etf symbols to an array of strings
func NewEtfList() ([]string, error) {

	var symbols []string

	buf, err := getAnonFTP("ftp.nasdaqtrader.com", "21", "symboldirectory", "otherlisted.txt")
	if err != nil {
		Log.Println(err)
		return symbols, err
	}

	for _, line := range strings.Split(string(buf), "\n") {
		// ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
		cols := strings.Split(line, "|")
		if len(cols) > 5 && cols[4] == "Y" && cols[6] == "N" {
			symbols = append(symbols, strings.ToLower(cols[0]))
		}
	}
	sort.Strings(symbols)
	return symbols, nil
}

// NewEtfFile - download a list of etf symbols to a file
func NewEtfFile(filename string) error {
	if filename == "" {
		filename = "etf.txt"
	}
	etfs, err := NewEtfList()
	if err != nil {
		return err
	}
	ba := []byte(strings.Join(etfs, "\n"))
	return ioutil.WriteFile(filename, ba, 0644)
}

// ValidMarkets list of markets that can be downloaded
var ValidMarkets = [...]string{"etf",
	"nasdaq",
	"nyse",
	"amex",
	"megacap",
	"largecap",
	"midcap",
	"smallcap",
	"microcap",
	"nanocap",
	"basicindustries",
	"capitalgoods",
	"consumerdurables",
	"consumernondurable",
	"consumerservices",
	"energy",
	"finance",
	"healthcare",
	"miscellaneous",
	"utilities",
	"technology",
	"transportation",
}

// ValidMarket - validate market string
func ValidMarket(market string) bool {
	if strings.HasPrefix(market, "tiingo") {
		if os.Getenv("TIINGO_API_TOKEN") == "" {
			fmt.Println("ERROR: Requires TIINGO_API_TOKEN to be set")
			return false
		}
	}
	for _, v := range ValidMarkets {
		if v == market {
			return true
		}
	}
	return false
}

// NewMarketList - download a list of market symbols to an array of strings
func NewMarketList(market string) ([]string, error) {

	var symbols []string
	if !ValidMarket(market) {
		return symbols, fmt.Errorf("invalid market")
	}
	var url string
	switch market {
	case "nasdaq":
		url = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download"
	case "amex":
		url = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download"
	case "nyse":
		url = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download"
	case "megacap":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?marketcap=Mega-cap&render=download"
	case "largecap":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?marketcap=Large-cap&render=download"
	case "midcap":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?marketcap=Mid-cap&render=download"
	case "smallcap":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?marketcap=Small-cap&render=download"
	case "microcap":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?marketcap=Micro-cap&render=download"
	case "nanocap":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?marketcap=Nano-cap&render=download"
	case "basicindustries":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Basic%20Industries&render=download"
	case "capitalgoods":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Capital%20Goods&render=download"
	case "consumerdurables":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Consumer%20Durables&render=download"
	case "consumernondurable":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Consumer%20Non-Durables&render=download"
	case "consumerservices":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Consumer%20Services&render=download"
	case "energy":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Energy&render=download"
	case "finance":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Finance&render=download"
	case "healthcare":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Health-Care&render=download"
	case "miscellaneous":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Miscellaneous&render=download"
	case "utilities":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Utilities&render=download"
	case "technology":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Technology&render=download"
	case "transportation":
		url = "http://www.nasdaq.com/screening/companies-by-industry.aspx?industry=Transportation&render=download"
	}

	resp, err := http.Get(url)
	if err != nil {
		return symbols, err
	}
	defer resp.Body.Close()

	var csvdata [][]string
	reader := csv.NewReader(resp.Body)
	csvdata, err = reader.ReadAll()
	if err != nil {
		return symbols, err
	}

	r, _ := regexp.Compile("^[a-z]+$")
	for row := 1; row < len(csvdata); row++ {
		sym := strings.TrimSpace(strings.ToLower(csvdata[row][0]))
		if r.MatchString(sym) {
			symbols = append(symbols, sym)
		}
	}
	sort.Strings(symbols)
	return symbols, nil
}

// NewMarketFile - download a list of market symbols to a file
func NewMarketFile(market, filename string) error {

	if !ValidMarket(market) {
		return fmt.Errorf("invalid market")
	}
	if market == "allmarkets" {
		for _, m := range ValidMarkets {
			filename = m + ".txt"
			syms, err := NewMarketList(m)
			if err != nil {
				Log.Println(err)
			}
			ba := []byte(strings.Join(syms, "\n"))
			ioutil.WriteFile(filename, ba, 0644)
		}
		return nil
	}

	// default filename
	if filename == "" {
		filename = market + ".txt"
	}
	syms, err := NewMarketList(market)
	if err != nil {
		return err
	}
	ba := []byte(strings.Join(syms, "\n"))
	return ioutil.WriteFile(filename, ba, 0644)
}

// NewSymbolsFromFile - read symbols from a file
func NewSymbolsFromFile(filename string) ([]string, error) {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		return []string{}, err
	}

	a := strings.Split(strings.ToLower(string(raw)), "\n")

	return deleteEmpty(a), nil
}

func deleteEmpty(s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}

// delete empty strings from a string array
func delete_empty(s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}

// Grab a file via anonymous FTP
func getAnonFTP(addr, port string, dir string, fname string) ([]byte, error) {

	var err error
	var contents []byte
	const timeout = 5 * time.Second

	nconn, err := net.DialTimeout("tcp", addr+":"+port, timeout)
	if err != nil {
		return contents, err
	}
	defer nconn.Close()

	conn := textproto.NewConn(nconn)
	_, _, _ = conn.ReadResponse(2)
	defer conn.Close()

	_ = conn.PrintfLine("USER anonymous")
	_, _, _ = conn.ReadResponse(0)

	_ = conn.PrintfLine("PASS anonymous")
	_, _, _ = conn.ReadResponse(230)

	_ = conn.PrintfLine("CWD %s", dir)
	_, _, _ = conn.ReadResponse(250)

	_ = conn.PrintfLine("PASV")
	_, message, _ := conn.ReadResponse(1)

	// PASV response format : 227 Entering Passive Mode (h1,h2,h3,h4,p1,p2).
	start, end := strings.Index(message, "("), strings.Index(message, ")")
	s := strings.Split(message[start:end], ",")
	l1, _ := strconv.Atoi(s[len(s)-2])
	l2, _ := strconv.Atoi(s[len(s)-1])
	dport := l1*256 + l2

	_ = conn.PrintfLine("RETR %s", fname)
	_, _, err = conn.ReadResponse(1)
	dconn, err := net.DialTimeout("tcp", addr+":"+strconv.Itoa(dport), timeout)
	defer dconn.Close()

	contents, err = ioutil.ReadAll(dconn)
	if err != nil {
		return contents, err
	}

	_ = dconn.Close()
	_, _, _ = conn.ReadResponse(2)

	return contents, nil
}
