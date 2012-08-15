function(doc) {
    if (doc.type == "collection") {
        emit([doc._id, 0], null)
        for (i in doc.item_tiids) {
            emit([doc._id, 1], {_id: doc.item_tiids[i]})
        }
    }
}