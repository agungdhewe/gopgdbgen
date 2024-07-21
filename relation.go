package gopgdbgen

import (
	"fmt"
	"os"
	"path"
)

type Relation struct {
	Name     string `json:"name"`
	Table    string
	Key      string `json:"key"`
	RefTable string `json:"reftable"`
	RefKey   string `json:"refkey"`
}

var RelationProp []string = []string{"key", "name", "table", "field"}

type TableRelations struct {
	Relations []*Relation
}

func NewTableRelations() *TableRelations {
	return &TableRelations{}
}

func (t *TableRelations) Add(r []*Relation) {
	t.Relations = append(t.Relations, r...)
}

func (t *TableRelations) WriteSql(sqlfilepath string) error {
	fmt.Println("writing sql file:", path.Base(sqlfilepath))
	file, err := os.Create(sqlfilepath)
	if err != nil {
		return err
	}

	fmt.Fprintln(file, "-- DDL Generator : Relation --")
	for _, rel := range t.Relations {
		fmt.Fprintf(file, "alter table %s add foreign key (%s) references %s(%s);\n", rel.Table, rel.Key, rel.RefTable, rel.RefKey)
	}
	fmt.Fprintln(file, "")
	return nil
}
