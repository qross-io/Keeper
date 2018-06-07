package io.qross.model

import io.qross.util.{DataHub, DataTable}

object MessageBox {
    
    def check(): DataTable = {
        val dh = new DataHub()
        val table =
            dh.get("SELECT id, sender, message_type, message_key, message_text FROM qross_message_box ORDER BY ID ASC LIMIT 10")
                .put("DELETE FROM qross_message_box WHERE id=#id")
                    .takeOut()
        dh.close()
        
        table
    }
    
}
