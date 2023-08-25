package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type Data struct {
	OrganizationCode string `json:"organizationCode"`
	GasStationCode   string `json:"gasStationCode"`
	DBIp             string `json:"dbIP"`
	DBPort           string `json:"dbPort"`
	DBDatabase       string `json:"dbDatabase"`
	DBUser           string `json:"dbUser"`
	DBPwd            string `json:"dbPwd"`
	DBRole           string `json:"dbRole"`
	DBCompanyID      string `json:"dbCompanyID"`
}

type PumpRows struct {
	CdAbastecimento int
	DhAbastecimento time.Time
	QtVolume        float64
	VlUnitario      float64
	VlTotal         float64
	FlLancado       int
	DsBico          string
	NrBico          int
	DsApelido       string
}

type PumpRowsData struct {
	GasStationTransactionID int
	Quantity                float64
	UnitValue               float64
	TotalValue              float64
	Processed               int
	Date                    string
	PumpNumber              int
	FuelName                string
	CompanyName             string
}

type PumpsData struct {
	OrganizationCode string
	GasStationCode   string
	PumpRowsData     []PumpRowsData
}

func main() {
	sendDataPeriodically()
}

func clearConsole() {
	cmd := exec.Command("clear")
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "cls")
	}
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		log.Println("Error clearing console:", err)
	}
}

func sendData(pumpsData PumpsData) {
	clearConsole()
	log.Println("Sending Data to:", pumpsData.OrganizationCode, pumpsData.GasStationCode)
	jsonData, err := json.Marshal(pumpsData)
	if err != nil {
		log.Println("Error marshaling data:", err)
		return
	}
	req, err := http.NewRequest("POST", "http://localhost:5000/pumps", bytes.NewBuffer(jsonData))
	// req, err := http.NewRequest("POST", "https://postos-api.touchsistemas.com.br/pumps", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Println("Error creating request:", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		log.Println("Status Code:", resp.StatusCode)
	} else {
		prettifiedData, err := json.MarshalIndent(pumpsData, "", "  ")
		if err != nil {
			log.Println("Error marshaling data:", err)
		} else {
			log.Println("Data sent successfully:", string(prettifiedData))
		}
	}
}

func readDatabase() {
	data := map[string]string{
		"user":             "sys",
		"dbname":           "gasstation",
		"password":         "Sys2023",
		"host":             "179.127.165.137",
		"port":             "5432",
		"role":             "dah",
		"companyID":        "1",
		"OrganizationCode": "fernandinho",
		"GasStationCode":   "posto_itajai",
	}
	pgConnStr := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=disable", data["user"], data["dbname"], data["password"], data["host"], data["port"])

	pgDB, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Println(err)
		return
	}
	defer pgDB.Close()
	_, err = pgDB.Exec(fmt.Sprintf("SET SESSION AUTHORIZATION %s", pq.QuoteIdentifier(data["role"])))
	if err != nil {
		log.Fatal(err)
		return
	}
	pgQuery := fmt.Sprintf(`SELECT
								a.cdabastecimento,
								a.dhabastecimento,
								a.qtvolume,
								a.vlunitario,
								a.vltotal,
								a.fllancado,
								b.dsbico,
								b.nrbico,
								e.dsapelido
							FROM
								dah.abastecimento a
							JOIN
								dah.bico b ON a.cdbico = b.cdbico
							JOIN
								dah.empresa e ON a.cdempresa = e.cdempresa
							WHERE
								a.fllancado = 0 and
								a.flcancelado = 0 and
								a.fltipo = 0 and
								a.dhabastecimento >= (NOW() - INTERVAL '12 hours' ) and
								a.cdempresa = %s
							ORDER BY
								a.dhprocessamento DESC`, data["companyID"])
	rows, err := pgDB.Query(pgQuery)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	pumpRowData := []PumpRowsData{}
	for rows.Next() {
		pumpRow := PumpRows{}
		err := rows.Scan(
			&pumpRow.CdAbastecimento, &pumpRow.DhAbastecimento,
			&pumpRow.QtVolume, &pumpRow.VlUnitario, &pumpRow.VlTotal,
			&pumpRow.FlLancado, &pumpRow.DsBico, &pumpRow.NrBico,
			&pumpRow.DsApelido,
		)
		if err != nil {
			log.Println(err)
			continue
		}
		pumpRowData = append(pumpRowData, PumpRowsData{
			GasStationTransactionID: pumpRow.CdAbastecimento,
			Quantity:                pumpRow.QtVolume,
			UnitValue:               pumpRow.VlUnitario,
			TotalValue:              pumpRow.VlTotal,
			Processed:               pumpRow.FlLancado,
			Date:                    pumpRow.DhAbastecimento.Format("2006-01-02 15:04:05"),
			PumpNumber:              pumpRow.NrBico,
			FuelName:                pumpRow.DsBico,
			CompanyName:             pumpRow.DsApelido,
		})
	}
	err = rows.Err()
	if err != nil {
		log.Println(err)
	}
	pumpsData := PumpsData{
		OrganizationCode: data["OrganizationCode"],
		GasStationCode:   data["GasStationCode"],
		PumpRowsData:     pumpRowData,
	}
	sendData(pumpsData)
}

func sendDataPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		readDatabase()
	}
}
