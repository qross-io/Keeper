package io.qross.keeper

import io.qross.model.{QrossNote, WorkActor, Process}

class NotePerformer extends WorkActor {
    override def process(process: Process): Unit = {
        QrossNote.perform(process)
    }
}
