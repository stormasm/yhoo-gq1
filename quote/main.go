/*
Copyright 2018 Mark Chenoweth
Licensed under terms of MIT license
*/

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/stormasm/yhoo-gq1"
)

const (
	version    = "0.2"
	dateFormat = "2006-01-02"
)

type quoteflags struct {
	years   int
	delay   int
	start   string
	end     string
	period  string
	source  string
	token   string
	infile  string
	outfile string
	format  string
	log     string
	all     bool
	adjust  bool
	version bool
}

func check(e error) {
	if e != nil {
		fmt.Printf("\nerror: %v\n\n", e)
		os.Exit(0)
	}
}

func checkFlags(flags quoteflags) error {

	// validate source
	if flags.source != "yahoo" &&
		flags.source != "google" {
		return fmt.Errorf("invalid source, must be either 'yahoo', 'google'")
	}

	// validate period
	if flags.source == "yahoo" &&
		(flags.period == "1m" || flags.period == "5m" || flags.period == "15m" || flags.period == "30m" || flags.period == "1h") {
		return fmt.Errorf("invalid source for yahoo, must be 'd'")
	}
	return nil
}

func setOutput(flags quoteflags) error {
	var err error
	if flags.log == "stdout" {
		quote.Log.SetOutput(os.Stdout)
	} else if flags.log == "stderr" {
		quote.Log.SetOutput(os.Stderr)
	} else if flags.log == "discard" {
		quote.Log.SetOutput(ioutil.Discard)
	} else {
		var f *os.File
		f, err = os.OpenFile(flags.log, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer f.Close()
		quote.Log.SetOutput(f)
	}
	return err
}

func getSymbols(flags quoteflags, args []string) ([]string, error) {

	var err error
	var symbols []string

	if flags.infile != "" {
		symbols, err = quote.NewSymbolsFromFile(flags.infile)
		if err != nil {
			return symbols, err
		}
	} else {
		symbols = args
	}

	// make sure we found some symbols
	if len(symbols) == 0 {
		return symbols, fmt.Errorf("no symbols specified")
	}

	// validate outfileFlag
	if len(symbols) > 1 && flags.outfile != "" && !flags.all {
		return symbols, fmt.Errorf("outfile not valid with multiple symbols\nuse -all=true")
	}

	return symbols, nil
}

func getPeriod(periodFlag string) quote.Period {
	period := quote.Daily
	switch periodFlag {
	case "1m":
		period = quote.Min1
	case "3m":
		period = quote.Min3
	case "5m":
		period = quote.Min5
	case "15m":
		period = quote.Min15
	case "30m":
		period = quote.Min30
	case "1h":
		period = quote.Min60
	case "2h":
		period = quote.Hour2
	case "4h":
		period = quote.Hour4
	case "6h":
		period = quote.Hour6
	case "8h":
		period = quote.Hour8
	case "12h":
		period = quote.Hour12
	case "d":
		period = quote.Daily
	case "1d":
		period = quote.Daily
	case "3d":
		period = quote.Day3
	case "w":
		period = quote.Weekly
	case "1w":
		period = quote.Weekly
	case "m":
		period = quote.Monthly
	case "1M":
		period = quote.Monthly
	}
	return period
}

func getTimes(flags quoteflags) (time.Time, time.Time) {
	// determine start/end times
	to := quote.ParseDateString(flags.end)
	var from time.Time
	if flags.start != "" {
		from = quote.ParseDateString(flags.start)
	} else { // use years
		from = to.Add(-time.Duration(int(time.Hour) * 24 * 365 * flags.years))
	}
	return from, to
}

func outputAll(symbols []string, flags quoteflags) error {
	// output all in one file
	from, to := getTimes(flags)
	period := getPeriod(flags.period)
	quotes := quote.Quotes{}
	var err error
	if flags.source == "yahoo" {
		quotes, err = quote.NewQuotesFromYahooSyms(symbols, from.Format(dateFormat), to.Format(dateFormat), period, flags.adjust)
	}
	if err != nil {
		return err
	}

	if flags.format == "csv" {
		err = quotes.WriteCSV(flags.outfile)
	} else if flags.format == "json" {
		err = quotes.WriteJSON(flags.outfile, false)
	}
	return err
}

func outputIndividual(symbols []string, flags quoteflags) error {
	// output individual symbol files

	from, to := getTimes(flags)
	period := getPeriod(flags.period)

	for _, sym := range symbols {
		var q quote.Quote
		if flags.source == "yahoo" {
			q, _ = quote.NewQuoteFromYahoo(sym, from.Format(dateFormat), to.Format(dateFormat), period, flags.adjust)
		}
		var err error
		if flags.format == "csv" {
			err = q.WriteCSV(flags.outfile)
		} else if flags.format == "json" {
			err = q.WriteJSON(flags.outfile, false)
		}
		if err != nil {
			fmt.Printf("Error writing file: %v\n", err)
		}
		time.Sleep(quote.Delay * time.Millisecond)
	}
	return nil
}

func main() {

	var err error
	var symbols []string
	var flags quoteflags

	flag.IntVar(&flags.years, "years", 5, "number of years to download")
	flag.IntVar(&flags.delay, "delay", 100, "milliseconds to delay between requests")
	flag.StringVar(&flags.start, "start", "", "start date (yyyy[-mm[-dd]])")
	flag.StringVar(&flags.end, "end", "", "end date (yyyy[-mm[-dd]])")
	flag.StringVar(&flags.period, "period", "d", "1m|5m|15m|30m|1h|d")
	flag.StringVar(&flags.source, "source", "yahoo", "yahoo|google|tiingo|gdax|bittrex|binance")
	flag.StringVar(&flags.token, "token", os.Getenv("TIINGO_API_TOKEN"), "tiingo api token")
	flag.StringVar(&flags.infile, "infile", "", "input filename")
	flag.StringVar(&flags.outfile, "outfile", "", "output filename")
	flag.StringVar(&flags.format, "format", "csv", "csv|json")
	flag.StringVar(&flags.log, "log", "stdout", "<filename>|stdout")
	flag.BoolVar(&flags.all, "all", false, "all output in one file")
	flag.BoolVar(&flags.adjust, "adjust", true, "adjust Yahoo prices")
	flag.BoolVar(&flags.version, "v", false, "show version")
	flag.BoolVar(&flags.version, "version", false, "show version")
	flag.Parse()

	if flags.version {
		fmt.Println(version)
		os.Exit(0)
	}

	quote.Delay = time.Duration(flags.delay)

	err = setOutput(flags)
	check(err)

	err = checkFlags(flags)
	check(err)

	symbols, err = getSymbols(flags, flag.Args())
	check(err)

	// main output
	if flags.all {
		err = outputAll(symbols, flags)
	} else {
		err = outputIndividual(symbols, flags)
	}
}
