package gopgdbgen

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"

	_ "github.com/jackc/pgx/v5/stdlib"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Database *struct {
		Host     string `yml:"host"`
		Port     int    `yml:"port"`
		Dbname   string `yml:"dbname"`
		Username string `yml:"username"`
		Password string `yml:"password"`
	} `yml:"database"`
	Directories *struct {
		Ddl string `yml:"./dbbuild/ddl"`
		Tbl string `yml:"./dbbuild/tbl"`
	} `yml:"directories"`
}

type DbGenerator struct {
	Config *Config
}

var dbg *DbGenerator

func NewGenerator() *DbGenerator {
	dbg = &DbGenerator{}
	return dbg
}

func (dbg *DbGenerator) ReadConfiguration(configfilepath string) {
	filedata, err := os.ReadFile(configfilepath)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(filedata, &dbg.Config)
	if err != nil {
		panic(err)
	}

	dir := path.Dir(configfilepath)
	dbg.Config.Directories.Ddl = path.Join(dir, dbg.Config.Directories.Ddl)
	dbg.Config.Directories.Tbl = path.Join(dir, dbg.Config.Directories.Tbl)
}

func (dbg *DbGenerator) GenerateAll() ([]string, error) {
	// https://stackoverflow.com/questions/68661084/original-order-of-json-values

	//ClearDirectory(dbg.Config.Directories.Ddl)

	files, err := os.ReadDir(dbg.Config.Directories.Tbl)
	if err != nil {
		log.Fatal(err)
	}

	relations := NewTableRelations()
	sqlfiles := []string{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileName := file.Name()
		jsontblpath := path.Join(dbg.Config.Directories.Tbl, fileName)
		sqlfilename := fmt.Sprintf("%s.sql", fileName[:len(fileName)-len(filepath.Ext(fileName))])
		sqlfilepath := path.Join(dbg.Config.Directories.Ddl, sqlfilename)

		tbl, err := dbg.ComposeTableFromJson(jsontblpath)
		if err != nil {
			return nil, err
		}

		err = tbl.WriteSql(sqlfilepath)
		if err != nil {
			eFileName := fmt.Sprintf("%s%s%s", ColorRed, path.Base(sqlfilepath), ColorReset)
			fmt.Println("ada kesalahan saat generate file", eFileName)
			return nil, err
		}

		rels := tbl.GetRelations()
		relations.Add(rels)
		sqlfiles = append(sqlfiles, sqlfilepath)
	}

	sqlfilepath := path.Join(dbg.Config.Directories.Ddl, "relation.sql")
	err = relations.WriteSql(sqlfilepath)
	if err != nil {
		eFileName := fmt.Sprintf("%s%s%s", ColorRed, path.Base(sqlfilepath), ColorReset)
		fmt.Println("ada kesalahan saat generate file", eFileName)
		return nil, err
	}

	sqlfiles = append(sqlfiles, sqlfilepath)
	return sqlfiles, nil

}

func (dbg *DbGenerator) ComposeTableFromJson(filepath string) (tbl *Table, err error) {
	filedata, err := os.ReadFile(filepath)
	if err != nil {
		return tbl, err
	}

	tbl, err = NewTable(filedata)
	if err != nil {
		eFile := fmt.Sprintf("%s%s%s", ColorRed, path.Base(filepath), ColorReset)
		return tbl, fmt.Errorf("%s\nada kesalahan di file %s", err, eFile)
	}

	return tbl, nil
}

func (dbg *DbGenerator) ClearDdlDirectory() error {
	dir := dbg.Config.Directories.Ddl

	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !file.IsDir() {
			filepath := path.Join(dir, file.Name())
			err := os.Remove(filepath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (dbg *DbGenerator) BuildDatabase(sqlfiles []string) error {
	conf := dbg.Config.Database
	fmt.Println("Connecting to DB..")
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s", conf.Host, conf.Port, conf.Dbname, conf.Username, conf.Password)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	fmt.Println("Database Connected.")
	fmt.Println("Executing DDL ...")
	for _, sqlfile := range sqlfiles {
		fmt.Println(sqlfile)
		query, err := os.ReadFile(sqlfile)
		if err != nil {
			return err
		}

		_, err = db.Exec(string(query))
		if err != nil {
			return err
		}
	}
	return nil
}
