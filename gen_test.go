package model

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TInsert(assignments interface{}) string {
	fields, placeholders := []string{}, []string{}
	genInsertAssignments(assignments, &fields, &placeholders)
	return `INSERT INTO t (` + strings.Join(fields, ",") + `) VALUES (` + strings.Join(placeholders, ",") + `);`
}

func THas(conds interface{}) (string, []interface{}) {
	placeholders, args := []string{}, []interface{}{}
	genConditionsVar(conds, &placeholders, &args)
	return `SELECT EXISTS(SELECT 1 FROM t WHERE ` + strings.Join(placeholders, " AND ") + `);`, args
}

func TFetchRows(conds interface{}, qp QueryParametersInterface) (string, []interface{}) {
	placeholders, args, conditions, hasPreCondition := []string{}, []interface{}{}, "", false

	if conds != nil && !reflect.ValueOf(conds).IsNil() {
		genConditionsVar(conds, &placeholders, &args)
		conditions = ` WHERE ` + strings.Join(placeholders, " AND ")
		hasPreCondition = true
	}

	if qp != nil {
		conditions += qp.Build(&args, hasPreCondition)
	}

	return `SELECT * FROM t` + conditions + `;`, args
}

func TFetchRow(conds interface{}) (string, []interface{}) {
	placeholders, args := []string{}, []interface{}{}
	genConditionsVar(conds, &placeholders, &args)
	return `SELECT * FROM t WHERE ` + strings.Join(placeholders, " AND ") + `;`, args
}

func TUpdate(conds, assignments interface{}) (string, map[string]interface{}) {
	phAssigns, phConditions, args := []string{}, []string{}, map[string]interface{}{}
	genConditionsNamed(assignments, &phAssigns, &args)
	genConditionsNamed(conds, &phConditions, &args)
	return `UPDATE t SET ` + strings.Join(phAssigns, ", ") + ` WHERE ` + strings.Join(phConditions, " AND ") + `;`, args
}

func TDelete(conds interface{}) string {
	placeholders := []string{}
	genConditionsNamed(conds, &placeholders, nil)
	return `DELETE FROM t WHERE ` + strings.Join(placeholders, " AND ") + `;`
}

type Row struct {
	Prototype `dive:"-"`
	Name      string `dbcol:"name" db:"name"`
	Phone     string `dbcol:"phone" db:"phone"`
}

type Assignments struct {
	Name  **string `dbcol:"name" db:"n_name"`
	Phone string   `dbcol:"phone" db:"n_phone"`
}

type QPTest struct {
	*QueryParameters
	Status string `db:"status"`
	From   string `db:"started_at" dbop:"<="`
	To     string `db:"expired_at" dbop:">="`
}

func TestGen(t *testing.T) {
	r := &Row{Name: "Boss", Phone: "0987654321"}
	r.Init()
	name := "John"
	ptrName := &name
	assignments := &Assignments{Name: &ptrName, Phone: "PHONE"}
	qp := &QPTest{
		QueryParameters: &QueryParameters{
			Paginate:    true,
			SearchQuery: "term",
			SearchCols:  []string{"title", "description"},
			OrderBy:     "ctime DESC",
			Pagination: Pagination{
				Offset: 2,
				Length: 97,
			},
		},
		Status: "DRAFTING",
		From:   "1970-01-01",
		To:     "2038-12-20",
	}

	var argv []interface{}
	var argn map[string]interface{}
	var q string

	//
	fmt.Println("[Insert]:", TInsert(r))
	fmt.Println("--------")

	//
	q, argv = THas(r)
	fmt.Println("[Has]:", q)
	for _, v := range argv {
		fmt.Println(v)
	}
	fmt.Println("--------")

	//
	q, argv = TFetchRows(assignments, qp)
	fmt.Println("[FetchRows 1]:", q)
	for _, v := range argv {
		fmt.Println(v)
	}
	fmt.Println("--------")

	q, argv = TFetchRows(nil, nil)
	fmt.Println("[FetchRows 2]:", q)
	for _, v := range argv {
		fmt.Println(v)
	}
	fmt.Println("--------")

	//
	q, argv = TFetchRow(r)
	fmt.Println("[FetchRow]:", q)
	for _, v := range argv {
		fmt.Println(v)
	}
	fmt.Println("--------")

	//
	q, argn = TUpdate(r, assignments)
	fmt.Println("[Update]:", q)
	for k, v := range argn {
		fmt.Println(k, "=>", v)
	}
	fmt.Println("--------")

	//
	fmt.Println("[Delete]:", TDelete(r))
}
