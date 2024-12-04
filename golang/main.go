package main

import (
	"encoding/csv"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/BehzadE/go-wikidump/pkg/wikidump"
)

func main() {

	if len(os.Args) != 3 {
		fmt.Println("usage: wikidump wikidump_data_dir outdir")
		os.Exit(1)
	}

	datadir := os.Args[1]
	outdir := os.Args[2]

	var streamfiles []string

	err := filepath.Walk(datadir, func(path string, info fs.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		base := filepath.Base(path)
		match, err := filepath.Match("*.xml*.bz2", base)
		if err != nil {
			return err
		}

		if match {
			// stream file
			streamfiles = append(streamfiles, base)
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	d, err := wikidump.New(datadir)
	if err != nil {
		log.Fatal(err)
	}

	err = d.PopulateDB()
	if err != nil {
		log.Fatal(err)
	}

	for _, streamfile := range streamfiles {
		base := filepath.Base(streamfile)
		outfile := filepath.Join(outdir, base[0:len(base)-4]+".csv")

		reader, err := d.NewStreamReader(base)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Print("Processing %s...\n", base)
		func() {
			f, err := os.Create(outfile)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			w := csv.NewWriter(f)

			for reader.Next() {
				b, err := reader.Read()
				if err != nil {
					log.Fatal(err)
				}
				pages, err := wikidump.ParseStream(b)
				if err != nil {
					log.Fatal(err)
				}
				for _, page := range pages {
					if len(page.Redirect.Title) > 0 {
						continue
					}

					rec := []string{strconv.FormatInt(page.ID, 10), page.Title, page.Revision.Text}

					err := w.Write(rec)
					if err != nil {
						log.Fatal(err)
					}

				}
			}
		}()

	}
}
