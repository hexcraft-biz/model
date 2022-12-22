package model

import (
	"database/sql"
	"fmt"
	"github.com/hexcraft-biz/misc/xtime"
	"github.com/hexcraft-biz/misc/xuuid"
	"github.com/jmoiron/sqlx"
	"reflect"
	"strings"
	"time"
)

const (
	TagDB       = "db"
	TagOperator = "dbop"
	TagCol      = "dbcol"
	TagDive     = "dive"

	MysqlErrCodeDuplicateEntry                  = 1062
	MysqlErrCodeIncorrectValue                  = 1411
	MysqlErrCodeForeignKeyConstraintFailsCreate = 1452
	MysqlErrCodeForeignKeyConstraintFailsDelete = 1451

	MysqlIntMax  = 2147483647
	MysqlUintMax = 4294967295
)

var (
	MysqlDatetimeMin xtime.Time = xtime.Time(time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC))
	MysqlDatetimeMax xtime.Time = xtime.Time(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC))
)

// ================================================================
// Prototype
// ================================================================
type PrototypeInterface interface {
	Init()
}

type PrototypeTime struct {
	Ctime *xtime.Time `db:"ctime" json:"createdAt"`
	Mtime *xtime.Time `db:"mtime" json:"modifiedAt"`
}

func (pt *PrototypeTime) Init() {
	ct := xtime.NowUTC()
	pt.Ctime = &ct
	pt.Mtime = &ct
}

type Prototype struct {
	ID            *xuuid.UUID `db:"id" json:"id"`
	PrototypeTime `dive:"-"`
}

func (p *Prototype) Init() {
	id := xuuid.New()
	p.ID = &id
	p.PrototypeTime.Init()
}

// ================================================================
//
// ================================================================
type Engine struct {
	*sqlx.DB
	TblName string
}

func NewEngine(db *sqlx.DB, tblName string) *Engine {
	return &Engine{
		DB:      db,
		TblName: tblName,
	}
}

type FetchVarArgumentsInterface interface {
	FetchVarArguments(argv *[]interface{}, placeholders *[]string)
}

type FetchNamedArgumentsInterface interface {
	FetchNamedArguments(argv *map[string]interface{}, placeholders *[]string)
}

type EngineInterface interface {
	NewRows() interface{}
	NewRow() interface{}
	Insert(assignments interface{}) (sql.Result, error)
	Has(conds interface{}) (bool, error)
	FetchRows(dest, conds interface{}, qp QueryParametersInterface) error
	FetchRow(dest, conds interface{}) error
	Update(conds, assignments interface{}) (sql.Result, error)
	Delete(conds interface{}) (sql.Result, error)
}

func (e *Engine) NewRows() interface{} {
	return &[]*Prototype{}
}

func (e *Engine) NewRow() interface{} {
	return new(Prototype)
}

func (e *Engine) Insert(assignments interface{}) (sql.Result, error) {
	fields, placeholders := []string{}, []string{}
	genInsertAssignments(assignments, &fields, &placeholders)
	q := `INSERT INTO ` + e.TblName + ` (` + strings.Join(fields, ",") + `) VALUES (` + strings.Join(placeholders, ",") + `);`
	return e.NamedExec(q, assignments)
}

func (e *Engine) Has(conds interface{}) (bool, error) {
	flag, placeholders, args := false, []string{}, []interface{}{}
	genConditionsVar(conds, &placeholders, &args)
	q := `SELECT EXISTS(SELECT 1 FROM ` + e.TblName + ` WHERE ` + strings.Join(placeholders, " AND ") + `);`
	err := e.Get(&flag, q, args...)
	return flag, err
}

func (e *Engine) FetchRows(dest, conds interface{}, qp QueryParametersInterface) error {
	placeholders, args, conditions := []string{}, []interface{}{}, ""
	hasPreCondition, hasConds := false, false

	if conds != nil {
		switch reflect.ValueOf(conds).Kind() {
		case reflect.Ptr:
			if !reflect.ValueOf(conds).IsNil() {
				hasConds = true
			}
		case reflect.Struct:
			hasConds = true
		default:
			return fmt.Errorf("Invalid condition input.")
		}
	}

	if hasConds {
		genConditionsVar(conds, &placeholders, &args)
		if len(placeholders) > 0 {
			hasPreCondition = true
			conditions = ` WHERE ` + strings.Join(placeholders, " AND ")
		}
	}

	if qp != nil {
		conditions += qp.Build(&args, hasPreCondition)
	}

	q := `SELECT * FROM ` + e.TblName + conditions + `;`
	return e.Select(dest, q, args...)
}

func (e *Engine) FetchRow(dest, conds interface{}) error {
	placeholders, args := []string{}, []interface{}{}
	genConditionsVar(conds, &placeholders, &args)
	q := `SELECT * FROM ` + e.TblName + ` WHERE ` + strings.Join(placeholders, " AND ") + `;`
	return e.Get(dest, q, args...)
}

func (e *Engine) Update(conds, assignments interface{}) (sql.Result, error) {
	phAssigns, phConditions, args := []string{}, []string{}, map[string]interface{}{}
	genUpdateAssignments(assignments, &phAssigns, &args)
	genConditionsNamed(conds, &phConditions, &args)
	q := `UPDATE ` + e.TblName + ` SET ` + strings.Join(phAssigns, ", ") + ` WHERE ` + strings.Join(phConditions, " AND ") + `;`
	return e.NamedExec(q, args)
}

func (e *Engine) Delete(conds interface{}) (sql.Result, error) {
	placeholders := []string{}
	genConditionsNamed(conds, &placeholders, nil)
	q := `DELETE FROM ` + e.TblName + ` WHERE ` + strings.Join(placeholders, " AND ") + `;`
	return e.NamedExec(q, conds)
}

// ----------------------------------------------------------------
// QueryParameters
// ----------------------------------------------------------------
type QueryParametersInterface interface {
	Build(args *[]interface{}, hasPreCondition bool) string
	GenSearchCondition(args *[]interface{}, hasPreCondition bool) string
	GenOrderBy() string
	PaginationInterface
}

type QueryParameters struct {
	Paginate    bool
	SearchQuery string `form:"q" binding:"omitempty"`
	SearchCols  []string
	OrderBy     string
	Pagination
}

func (qp *QueryParameters) Build(args *[]interface{}, hasPreCondition bool) string {
	q := qp.GenSearchCondition(args, hasPreCondition) + qp.GenOrderBy()
	if qp.Paginate {
		q += qp.Pagination.Validate().ToString(args)
	}
	return q
}

func (qp *QueryParameters) GenSearchCondition(args *[]interface{}, hasPreCondition bool) string {
	conditions := ""
	if qp.SearchQuery != "" && len(qp.SearchCols) > 0 {
		for i := range qp.SearchCols {
			qp.SearchCols[i] += " LIKE ?"
			*args = append(*args, "%"+qp.SearchQuery+"%")
		}

		if hasPreCondition {
			conditions = ` AND (` + strings.Join(qp.SearchCols, " OR ") + `)`
		} else {
			conditions = ` ` + strings.Join(qp.SearchCols, " OR ")
		}
	}

	return conditions
}

func (qp *QueryParameters) GenOrderBy() string {
	if qp.OrderBy != "" {
		return ` ORDER BY ` + qp.OrderBy
	} else {
		return ``
	}
}

// ----------------------------------------------------------------
// Pagination
// ----------------------------------------------------------------
const (
	PaginationDefaultOffset = 0
	PaginationDefaultLength = 16
	PaginationMinLength     = 1
	PaginationMaxLength     = 1024
)

type PaginationInterface interface {
	ToString(args *[]interface{}) string
}

type Pagination struct {
	Offset int64 `form:"pos" binding:"omitempty"`
	Length int64 `form:"len" binding:"omitempty"`
}

func (p *Pagination) Validate() *Pagination {
	if p.Offset < 0 {
		p.Offset = PaginationDefaultOffset
	}

	if p.Length == 0 {
		p.Length = PaginationDefaultLength
	} else if p.Length < PaginationMinLength {
		p.Length = PaginationMinLength
	} else if p.Length > PaginationMaxLength {
		p.Length = PaginationMaxLength
	}

	return p
}

func (p *Pagination) ToString(args *[]interface{}) string {
	*args = append(*args, p.Offset, p.Length)
	return ` LIMIT ?, ?`
}

// ----------------------------------------------------------------
// Misc
// ----------------------------------------------------------------
func genInsertAssignments(assignments interface{}, fields, placeholders *[]string) {
	if v, isNil := getValuePointsTo(reflect.ValueOf(assignments)); !isNil {
		length := v.NumField()
		for i := 0; i < length; i++ {
			val, struF := v.Field(i), v.Type().Field(i)
			if val, isNil := getValuePointsTo(val); !isNil {
				if _, ok := struF.Tag.Lookup(TagDive); ok {
					genInsertAssignments(val.Interface(), fields, placeholders)
				} else if dbCol, _, _ := fetchDBTag(struF.Tag); isValidAssignment(val, dbCol) && !strings.Contains(dbCol, " ") {
					*fields = append(*fields, dbCol)
					*placeholders = append(*placeholders, fmt.Sprintf(":%s", dbCol))
				}
			}
		}
	}
}

func genUpdateAssignments(sour interface{}, placeholders *[]string, args *map[string]interface{}) {
	if v, isNil := getValuePointsTo(reflect.ValueOf(sour)); !isNil {
		length := v.NumField()
		for i := 0; i < length; i += 1 {
			val, struF := v.Field(i), v.Type().Field(i)
			if val, isNil := getValuePointsTo(val); !isNil {
				if _, ok := struF.Tag.Lookup(TagDive); ok {
					genConditionsNamed(val.Interface(), placeholders, args)
				} else if dbCol, _, dbVal := fetchDBTag(struF.Tag); isValidAssignment(val, dbCol) && !strings.Contains(dbCol, " ") {
					setNamedArg(args, dbVal, val)
					*placeholders = append(*placeholders, fmt.Sprintf("%s = :%s", dbCol, dbVal))
				}
			}
		}
	}
}

func genConditionsVar(sour interface{}, placeholders *[]string, args *[]interface{}) {
	if v, isNil := getValuePointsTo(reflect.ValueOf(sour)); !isNil {
		length := v.NumField()
		for i := 0; i < length; i += 1 {
			val, struF := v.Field(i), v.Type().Field(i)
			if val, isNil := getValuePointsTo(val); !isNil {
				if _, ok := struF.Tag.Lookup(TagDive); ok {
					genConditionsVar(val.Interface(), placeholders, args)
				} else if dbCol, operator, _ := fetchDBTag(struF.Tag); isValidAssignment(val, dbCol) {
					fmtStr, cols := "", strings.Split(dbCol, " ")
					colsLen := len(cols)
					switch colsLen {
					case 1:
						fmtStr = fmt.Sprintf("%s "+operator+" ?", cols[0])
						*args = append(*args, val.Interface())
					default:
						fmtStrs := make([]string, colsLen)
						for i := range cols {
							fmtStrs[i] = fmt.Sprintf("%s "+operator+" ?", cols[i])
							*args = append(*args, val.Interface())
						}
						fmtStr = "(" + strings.Join(fmtStrs, " OR ") + ")"
					}
					*placeholders = append(*placeholders, fmtStr)
				}
			}
		}
	}
}

func genConditionsNamed(sour interface{}, placeholders *[]string, args *map[string]interface{}) {
	if v, isNil := getValuePointsTo(reflect.ValueOf(sour)); !isNil {
		length := v.NumField()
		for i := 0; i < length; i += 1 {
			val, struF := v.Field(i), v.Type().Field(i)
			if val, isNil := getValuePointsTo(val); !isNil {
				if _, ok := struF.Tag.Lookup(TagDive); ok {
					genConditionsNamed(val.Interface(), placeholders, args)
				} else if dbCol, operator, dbVal := fetchDBTag(struF.Tag); isValidAssignment(val, dbCol) {
					fmtStr, cols := "", strings.Split(dbCol, " ")
					colsLen := len(cols)
					switch colsLen {
					case 1:
						fmtStr = fmt.Sprintf("%s "+operator+" :%s", cols[0], dbVal)
						setNamedArg(args, dbVal, val)
					default:
						fmtStrs := make([]string, colsLen)
						for i := range cols {
							fmtStrs[i] = fmt.Sprintf("%s "+operator+" :%s", cols[i], dbVal)
							setNamedArg(args, dbVal, val)
						}
						fmtStr = "(" + strings.Join(fmtStrs, " OR ") + ")"
					}
					*placeholders = append(*placeholders, fmtStr)
				}
			}
		}
	}
}

func getValuePointsTo(v reflect.Value) (reflect.Value, bool) {
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}

	return v, (v.Kind() == reflect.Ptr)
}

func setNamedArg(args *map[string]interface{}, dbVal string, val reflect.Value) {
	if args != nil {
		(*args)[dbVal] = val.Interface()
	}
}

func fetchDBTag(tag reflect.StructTag) (string, string, string) {
	col, val := tag.Get(TagCol), tag.Get(TagDB)
	if col == "" {
		col = val
	}

	operator := tag.Get(TagOperator)
	if operator == "" {
		operator = "="
	}

	return col, operator, val
}

func isValidAssignment(v reflect.Value, dbCol string) bool {
	if dbCol == "" || dbCol == "-" {
		return false
	}

	return true
}
