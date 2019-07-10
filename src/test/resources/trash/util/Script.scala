package io.qross.util

import javax.script.{ScriptEngine, ScriptEngineManager, ScriptException}


object Script {
    
    def eval(script: String, default: Double = 0D): Double = {
        val jse: ScriptEngine = new ScriptEngineManager().getEngineByName("JavaScript")
        try {
            jse.eval(script).toString.toDouble
        }
        catch {
            case e: ScriptException =>
                e.printStackTrace()
                default
        }
        
        //The code doesn't work below.
        //import scala.tools.nsc._
        //val interpreter = new Interpreter(new Settings())
        //interpreter.interpret("import java.text.{SimpleDateFormat => sdf}")
        //interpreter.bind("date", "java.util.Date", new java.util.Date());
        //interpreter.eval[String]("""new sdf("yyyy-MM-dd").format(date)""") get
        //    interpreter.close()
    }
}
