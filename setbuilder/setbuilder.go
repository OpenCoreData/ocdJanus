package setbuilder

import (
	"log"
	"opencoredata.org/ocdJanus/connect"
	"opencoredata.org/ocdJanus/measurements"
	"opencoredata.org/ocdJanus/queries"
)

func LHSLopper() {
	log.Println("In the LHSLopper")

	conn, err := connect.GetJanusCon()
	if err != nil {
		log.Printf(`Error with	: %s`, err)
		return
	}
	defer conn.Close()

	qry := queries.Sql_lsh5

	rows, err := conn.Query(qry)
	if err != nil {
		log.Printf(`Error with "%s": %s`, qry, err)
		return
	}

	var (
		resleg  string
		ressite string
		reshole string
	)

	for rows.Next() {
		if err = rows.Scan(&resleg, &ressite, &reshole); err != nil {
			log.Printf("Error fetching: %s", err)
			break
		}

		log.Printf(`lsh: %s_%s%s`, resleg, ressite, reshole)
		measurements.Measurements(resleg, ressite, reshole)
	}
}
