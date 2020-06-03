package io.qross.model

import java.io.File
import java.util.regex.Pattern

import io.qross.core.DataRow
import io.qross.ext.TypeExt._
import io.qross.jdbc.DataSource
import io.qross.net.Json

import scala.util.{Success, Try}

object TaskDependency {

    def check(depend: DataRow): String =  {
        
        var ready = "no"


        depend.getString("dependency_type").toUpperCase() match {

            /*
            {
                "jobId": 123,
                "taskTime": "datetime sharp expression",
                "status": "success"
            }
            */
            case "TASK" =>
                if (DataSource.QROSS.queryExists("SELECT id FROM qross_tasks WHERE job_id=? AND task_time=? AND status=?",
                    depend.getInt("dependency_label"), //job_id
                    depend.getDateTime("task_time").format(depend.getString("dependency_content")), //task_time format
                    depend.getString("dependency_option", TaskStatus.SUCCESS))) { //status
                    ready = "yes"
                }

            /*
                {
                    "dataSource": "source name"
                    "selectSQL": "select SQL"
                    "updateSQL": "update SQL, support # place holder",
                    "field": "", //如果不设置, 默认第一个字段
                    "operator": "==", //如果不设置，默认"="
                    "value": 0  //如果不设置，则有数据则依赖成立
                }             */
            case "SQL" =>
                val ds = new DataSource(depend.getString("dependency_label")) //dataSource
                val table = ds.executeDataTable(depend.getString("dependency_content")) //selectSQL
                if (table.nonEmpty) {
                    ready = "yes"


                    val updateSQL = depend.getString("dependency_option")
                    if (updateSQL != "") {
                        ds.tableUpdate(updateSQL, table)
                    }

                    //pass variables to command in pre-dependency
                    table.lastRow match {
                        case Some(row) =>
                            val df = DataSource.QROSS
                            row.getFields.foreach(field => {
                                df.addBatchCommand(s"UPDATE qross_tasks_dags SET command_text=REPLACE(command_text, '#{$field}', '${row.getString(field).replace("'", "''")}') WHERE task_id=${depend.getLong("task_id")} AND record_time='${depend.getString("record_time")}' AND POSITION('#{' IN command_text) > 0")
                            })
                            df.executeBatchCommands()
                            df.close()
                        case None =>
                    }

                    table.clear()
                }
                ds.close()

            case "PQL" =>
                

        }

        ready
    }
}