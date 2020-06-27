package io.qross.model

import java.io.File
import java.util.regex.Pattern

import io.qross.core.{DataCell, DataHub, DataRow, DataTable, DataType}
import io.qross.ext.TypeExt._
import io.qross.jdbc.DataSource
import io.qross.net.Json
import io.qross.pql.{PQL, Sharp}

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
                    depend.getString("dependency_content"), //task_time format
                    depend.getString("dependency_option", TaskStatus.SUCCESS))) {
                        //status
                        ready = "yes"
                }

            /*
                {
                    "dataSource": "source name"
                    "selectSQL": "select SQL"
                    "updateSQL": "update SQL, support # place holder"
                }             */
            case "SQL" =>
                val connection = depend.getString("dependency_label")
                val ds = {
                    try {
                        if (connection == "") {
                            new DataSource()
                        }
                        else {
                            new DataSource(connection)
                        }
                    }
                    catch {
                        case e: Exception =>
                            e.printStackTrace()
                            TaskRecorder.of(depend.getInt("job_id"), depend.getLong("task_id"), depend.getString("record_time"))
                                .warn(s"Wrong or no default dataSource name: " + connection)
                                .err(e.getMessage)
                            null
                    }
                }

                if (ds != null) {
                    val table = {
                        try {
                            ds.executeDataTable(depend.getString("dependency_content"))
                        }
                        catch {
                            case e: Exception =>
                                e.printStackTrace()
                                TaskRecorder.of(depend.getInt("job_id"), depend.getLong("task_id"), depend.getString("record_time"))
                                    .warn(s"Wrong SELECT sentence: " + depend.getString("dependency_content"))
                                    .err(e.getMessage)
                                new DataTable()
                        }
                    } //selectSQL

                    if (table.nonEmpty) {
                        ready = "yes"

                        val updateSQL = depend.getString("dependency_option")
                        if (updateSQL != "") {
                            try {
                                ds.tableUpdate(updateSQL, table)
                            }
                            catch {
                                case e: Exception =>
                                    e.printStackTrace()
                                    TaskRecorder.of(depend.getInt("job_id"), depend.getLong("task_id"), depend.getString("record_time"))
                                        .warn(s"Wrong non Query sentence: " + updateSQL)
                                        .err(e.getMessage)
                            }
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
                }
            case "PQL" =>
                /*
                通过 OUTPUT 的返回值  来判断是否依赖成功
                DATATABLE 是否为空
                DATAROW 是否为空
                列表 是否为空
                数字，大于0为TRUE
                布尔,，true或false
                字符串  yes no true false
                其他 不为null
                 */
                val dh = new DataHub(depend.getString("dependency_label"))
                val result = {
                    try {
                        new PQL(depend.getString("dependency_content"), dh)
                          .set(depend)
                          .output
                          .lastOption match {
                            case Some(value) => value.toBoolean(false)
                            case None => false
                        }
                    }
                    catch {
                        case e: Exception =>
                            e.printStackTrace()
                            TaskRecorder.of(depend.getInt("job_id"), depend.getLong("task_id"), depend.getString("record_time"))
                              .warn(s"Wrong PQL statement: " + depend.getString("dependency_content"))
                              .err(e.getMessage)
                            false
                    }
                }

                if (result) {
                    ready = "yes"

                    try {
                        new PQL(depend.getString("dependency_option"), dh).set(depend).run()
                    }
                    catch {
                        case e: Exception =>
                            e.printStackTrace()
                            TaskRecorder.of(depend.getInt("job_id"), depend.getLong("task_id"), depend.getString("record_time"))
                              .warn(s"Wrong PQL statement: " + depend.getString("dependency_option"))
                              .err(e.getMessage)
                    }
                }
        }

        ready
    }
}