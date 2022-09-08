package oracle

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	_ "github.com/godror/godror"
	"github.com/thoas/go-funk"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils"
)

type Config struct {
	DriverName        string
	DSN               string
	Conn              gorm.ConnPool
	DefaultStringSize uint
}

type Dialector struct {
	*Config
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (d Dialector) DummyTableName() string {
	return "DUAL"
}

func (d Dialector) Name() string {
	return "oracle"
}

func (d Dialector) Initialize(db *gorm.DB) (err error) {
	db.NamingStrategy = Namer{}
	d.DefaultStringSize = 1024

	config := &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT"},
		QueryClauses:  []string{"SELECT", "FROM", "WHERE", "GROUP BY", "LIMIT", "ORDER BY", "FOR"},
		UpdateClauses: []string{"UPDATE", "SET", "WHERE"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE"},
	}
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, config)

	d.DriverName = "godror"

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		db.ConnPool, err = sql.Open(d.DriverName, d.DSN)
	}

	createCallback := db.Callback().Create()
	if err = createCallback.Replace("gorm:before_create", BeforeCreate); err != nil {
		fmt.Printf("Replace gorm:before_create err: %+v \n", err)
		return
	}
	if err = createCallback.Replace("gorm:create", Create); err != nil {
		fmt.Printf("Replace gorm:create err: %+v \n", err)
		return
	}

	queryCallback := db.Callback().Query()
	if err = queryCallback.Replace("gorm:query", Query); err != nil {
		fmt.Printf("Replace gorm:query err: %+v \n", err)
		return
	}

	updateCallback := db.Callback().Update()
	if err = updateCallback.Replace("gorm:before_update", BeforeUpdate); err != nil {
		fmt.Printf("Replace gorm:before_update err: %+v \n", err)
		return
	}
	if err = updateCallback.Replace("gorm:update", Update(config)); err != nil {
		fmt.Printf("Replace gorm:update err: %+v \n", err)
		return
	}

	deleteCallback := db.Callback().Delete()
	if err = deleteCallback.Replace("gorm:before_delete", BeforeDelete); err != nil {
		fmt.Printf("Replace gorm:before_delete err: %+v \n", err)
		return
	}
	if err = deleteCallback.Replace("gorm:delete", Delete(config)); err != nil {
		fmt.Printf("Replace gorm:delete err: %+v \n", err)
		return
	}

	rowCallback := db.Callback().Row()
	if err = rowCallback.Replace("gorm:row", RowQuery); err != nil {
		fmt.Printf("Replace gorm:row err: %+v \n", err)
		return
	}

	rawCallback := db.Callback().Raw()
	if err = rawCallback.Replace("gorm:raw", Raw); err != nil {
		fmt.Printf("Replace gorm:raw err: %+v \n", err)
		return
	}

	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (d Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"LIMIT": d.RewriteLimit,
	}
}

func (d Dialector) RewriteLimit(c clause.Clause, builder clause.Builder) {
	if limit, ok := c.Expression.(clause.Limit); ok {
		if stmt, ok := builder.(*gorm.Statement); ok {
			if _, ok := stmt.Clauses["WHERE"]; !ok {
				builder.WriteString("WHERE 1 = 1 ")
			}
			builder.WriteString(" AND ROWNUM <= ")
			builder.WriteString(strconv.Itoa(limit.Limit))

			if limit.Offset > 0 {
				panic("Offset not supported.")
			}
		}
	}
}

func (d Dialector) DefaultValueOf(*schema.Field) clause.Expression {
	return clause.Expr{SQL: "VALUES (DEFAULT)"}
}

func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:                          db,
				Dialector:                   d,
				CreateIndexAfterCreateTable: true,
			},
		},
	}
}

func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteString(":")
	writer.WriteString(strconv.Itoa(len(stmt.Vars)))
}

func (d Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteString(str)
}

var numericPlaceholder = regexp.MustCompile(`:(\d+)`)

func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, numericPlaceholder, `'`, funk.Map(vars, func(v interface{}) interface{} {
		switch v := v.(type) {
		case bool:
			if v {
				return 1
			}
			return 0
		default:
			return v
		}
	}).([]interface{})...)
}

func (d Dialector) DataTypeOf(field *schema.Field) string {
	if _, found := field.TagSettings["RESTRICT"]; found {
		delete(field.TagSettings, "RESTRICT")
	}

	var sqlType string

	switch field.DataType {
	case schema.Bool, schema.Int, schema.Uint, schema.Float:
		sqlType = "INTEGER"

		switch {
		case field.DataType == schema.Float:
			sqlType = "FLOAT"
		case field.Size <= 8:
			sqlType = "SMALLINT"
		}

		if val, ok := field.TagSettings["AUTOINCREMENT"]; ok && utils.CheckTruth(val) {
			sqlType += " GENERATED BY DEFAULT AS IDENTITY"
		}
	case schema.String, "VARCHAR2":
		size := field.Size
		defaultSize := d.DefaultStringSize

		if size == 0 {
			if defaultSize > 0 {
				size = int(defaultSize)
			} else {
				hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
				// TEXT, GEOMETRY or JSON column can't have a default value
				if field.PrimaryKey || field.HasDefaultValue || hasIndex {
					size = 191 // utf8mb4
				}
			}
		}

		if size >= 2000 {
			sqlType = "CLOB"
		} else {
			sqlType = fmt.Sprintf("VARCHAR2(%d)", size)
		}

	case schema.Time:
		sqlType = "TIMESTAMP WITH TIME ZONE"
		if field.NotNull || field.PrimaryKey {
			sqlType += " NOT NULL"
		}
	case schema.Bytes:
		sqlType = "BLOB"
	default:
		sqlType = string(field.DataType)

		if strings.EqualFold(sqlType, "text") {
			sqlType = "CLOB"
		}

		if sqlType == "" {
			panic(fmt.Sprintf("invalid sql type %s (%s) for oracle", field.FieldType.Name(), field.FieldType.String()))
		}

		notNull, _ := field.TagSettings["NOT NULL"]
		unique, _ := field.TagSettings["UNIQUE"]
		additionalType := fmt.Sprintf("%s %s", notNull, unique)
		if value, ok := field.TagSettings["DEFAULT"]; ok {
			additionalType = fmt.Sprintf("%s %s %s%s", "DEFAULT", value, additionalType, func() string {
				if value, ok := field.TagSettings["COMMENT"]; ok {
					return " COMMENT " + value
				}
				return ""
			}())
		}
		sqlType = fmt.Sprintf("%v %v", sqlType, additionalType)
	}

	return sqlType
}

func (d Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return tx.Error
}

func (d Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return tx.Error
}

// TODO: 此改动将导致该驱动仅适用于大写的表名及字段名
func upperDBName(db *gorm.DB) {
	// upper table db name
	if db.Statement == nil {
		return
	}
	tableName := strings.Trim(db.Statement.Table, `"`)
	tableName = `"` + strings.ToUpper(tableName) + `"`
	db.Statement.Table = tableName

	// upper field db name
	schema := db.Statement.Schema
	if schema == nil {
		return
	}
	db.Statement.Schema.Table = tableName
	for i, dbName := range schema.DBNames {
		field := schema.FieldsByDBName[dbName]

		dbName = strings.Trim(dbName, `"`)
		dbName = `"` + strings.ToUpper(dbName) + `"`
		schema.DBNames[i] = dbName

		field.DBName = dbName
		schema.FieldsByDBName[dbName] = field
	}
}

func transferValType(db *gorm.DB) {
	if db.Statement == nil || db.Statement.Statement == nil {
		return
	}

	stmt := db.Statement.Statement
	for i, val := range stmt.Vars {
		switch val.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string:

		default:
			// 自定义类型在 godror 会报 unknown type (如：stmt.go 1242)
			switch reflect.TypeOf(val).Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				val = reflect.ValueOf(val).Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				val = reflect.ValueOf(val).Uint()
			case reflect.Float32, reflect.Float64:
				val = reflect.ValueOf(val).Float()
			case reflect.String:
				val = reflect.ValueOf(val).String()
			}
			stmt.Vars[i] = val
		}
	}
}
