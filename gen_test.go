package model

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/hexcraft-biz/xuuid"
	"github.com/jmoiron/sqlx"
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
	genUpdateAssignments(assignments, &phAssigns, &args)
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
	Name      string         `dbcol:"name" db:"name"`
	Phone     string         `dbcol:"phone" db:"phone"`
	Identity  xuuid.Wildcard `dbcol:"id name" db:"identity"`
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

//func TestGen(t *testing.T) {
//	r := &Row{Name: "Boss", Phone: "0987654321", Identity: xuuid.Wildcard{Type: xuuid.WildcardTypeXUUID, Val: xuuid.UUID(uuid.New())}}
//	r.Init()
//	name := "John"
//	ptrName := &name
//	assignments := &Assignments{Name: &ptrName, Phone: "PHONE"}
//	qp := &QPTest{
//		QueryParameters: &QueryParameters{
//			Paginate:    true,
//			SearchQuery: "term",
//			SearchCols:  []string{"title", "description"},
//			OrderBy:     "ctime DESC",
//			Pagination: Pagination{
//				Offset: 2,
//				Length: 97,
//			},
//		},
//		Status: "DRAFTING",
//		From:   "1970-01-01",
//		To:     "2038-12-20",
//	}
//
//	var argv []interface{}
//	var argn map[string]interface{}
//	var q string
//
//	//
//	fmt.Println("[Insert]:", TInsert(r))
//	fmt.Println("--------")
//
//	//
//	q, argv = THas(r)
//	fmt.Println("[Has]:", q)
//	for _, v := range argv {
//		fmt.Println(v)
//	}
//	fmt.Println("--------")
//
//	//
//	q, argv = TFetchRows(assignments, qp)
//	fmt.Println("[FetchRows 1]:", q)
//	for _, v := range argv {
//		fmt.Println(v)
//	}
//	fmt.Println("--------")
//
//	q, argv = TFetchRows(nil, nil)
//	fmt.Println("[FetchRows 2]:", q)
//	for _, v := range argv {
//		fmt.Println(v)
//	}
//	fmt.Println("--------")
//
//	q, argv = TFetchRows(nil, qp)
//	fmt.Println("[FetchRows 3]:", q)
//	for _, v := range argv {
//		fmt.Println(v)
//	}
//	fmt.Println("--------")
//
//	qp.QueryParameters.SearchQuery = ""
//	q, argv = TFetchRows(nil, qp)
//	fmt.Println("[FetchRows 4]:", q)
//	for _, v := range argv {
//		fmt.Println(v)
//	}
//	fmt.Println("--------")
//
//	//
//	q, argv = TFetchRow(r)
//	fmt.Println("[FetchRow 1]:", q)
//	for _, v := range argv {
//		fmt.Println(v)
//	}
//	fmt.Println("--------")
//
//	//
//	q, argn = TUpdate(r, assignments)
//	fmt.Println("[Update]:", q)
//	for k, v := range argn {
//		fmt.Println(k, "=>", v)
//	}
//	fmt.Println("--------")
//
//	//
//	fmt.Println("[Delete]:", TDelete(r))
//}

// ================================================================
//
// ================================================================
type ERewardTypes struct {
	*Engine
}

func NewERewardTypes(db *sqlx.DB) *ERewardTypes {
	return &ERewardTypes{
		Engine: NewEngine(db, "reward_types"),
	}
}

type RewardType struct {
	Prototype          `dive:"-"`
	Identity           string `db:"identity" json:"identity"`
	IssueQuantity      int    `db:"issue_quantity" json:"issueQuantity"`
	MaxAcquiredPerUser int    `db:"max_acquired_per_user" json:"maxAcquiredPerUser"`
	HasCallback        bool   `db:"has_callback" json:"hasCallback"`
	Title              string `db:"title" json:"title"`
	Description        string `db:"description" json:"description"`
}

type CommonAnchor struct {
	ID xuuid.UUID `uri:"id" binding:"required" db:"id"`
}

func TestFetch(t *testing.T) {
	protocol := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s")
	db, err := sqlx.Open("mysql", protocol)
	if err != nil {
		t.Fatal(err.Error())
	}

	anchor := &CommonAnchor{
		ID: xuuid.UUID(uuid.Must(uuid.Parse("5b080758-7ece-4be0-bf37-7f6da5fb1be6"))),
	}
	row := new(RewardType)
	if err := NewERewardTypes(db).FetchRow(row, anchor); err != nil {
		t.Fatal(err.Error())
	} else if js, err := json.MarshalIndent(row, "", "\t"); err != nil {
		t.Fatal(err.Error())
	} else {
		fmt.Println(string(js))
	}
}
