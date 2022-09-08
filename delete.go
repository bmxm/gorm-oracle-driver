package oracle

import (
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils"
	"reflect"
	"strings"
)

func BeforeDelete(db *gorm.DB) {
	callbacks.BeforeDelete(db)

	if db.Statement == nil {
		return
	}
	if destMap, ok := db.Statement.Dest.(map[string]interface{}); ok {
		newDestMap := make(map[string]interface{}, len(destMap))
		for key, val := range destMap {
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
			}
			key = strings.Trim(key, `"`)
			newDestMap[`"`+strings.ToUpper(key)+`"`] = val
		}
		db.Statement.Dest = newDestMap
	}
}

func Delete(config *callbacks.Config) func(db *gorm.DB) {
	supportReturning := utils.Contains(config.DeleteClauses, "RETURNING")

	return func(db *gorm.DB) {
		if db.Error != nil {
			return
		}

		if db.Statement.Schema != nil {
			for _, c := range db.Statement.Schema.DeleteClauses {
				db.Statement.AddClause(c)
			}
		}

		if db.Statement.SQL.Len() == 0 {
			db.Statement.SQL.Grow(100)
			db.Statement.AddClauseIfNotExists(clause.Delete{})

			if db.Statement.Schema != nil {
				_, queryValues := schema.GetIdentityFieldValuesMap(db.Statement.Context, db.Statement.ReflectValue, db.Statement.Schema.PrimaryFields)
				column, values := schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

				if len(values) > 0 {
					db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
				}

				if db.Statement.ReflectValue.CanAddr() && db.Statement.Dest != db.Statement.Model && db.Statement.Model != nil {
					_, queryValues = schema.GetIdentityFieldValuesMap(db.Statement.Context, reflect.ValueOf(db.Statement.Model), db.Statement.Schema.PrimaryFields)
					column, values = schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

					if len(values) > 0 {
						db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
					}
				}
			}

			db.Statement.AddClauseIfNotExists(clause.From{})

			db.Statement.Build(db.Statement.BuildClauses...)
			transferValType(db)
		}

		checkMissingWhereConditions(db)

		if !db.DryRun && db.Error == nil {
			ok, mode := hasReturning(db, supportReturning)
			if !ok {
				result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)
				if db.AddError(err) == nil {
					db.RowsAffected, _ = result.RowsAffected()
				}

				return
			}

			if rows, err := db.Statement.ConnPool.QueryContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...); db.AddError(err) == nil {
				gorm.Scan(rows, db, mode)
				db.AddError(rows.Close())
			}
		}
	}
}
