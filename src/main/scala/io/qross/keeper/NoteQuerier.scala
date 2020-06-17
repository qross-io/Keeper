package io.qross.keeper

import io.qross.model.{QrossNote, WorkActor}

class NoteQuerier extends WorkActor {
    override def process(noteId: Long, userId: Int): Unit = {
        QrossNote.query(noteId, userId)
    }
}