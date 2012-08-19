function(doc) {
    if (doc.type == "collection") {
        var coll_id = doc._id
        emit([doc._id, 0, 0], null)
        for (i in doc.item_tiids) {
            var item_id =  doc.item_tiids[i]
            emit([doc._id, item_id, 1], {_id: item_id})
        }
    }
    else if (doc.type == "metric_snap") {
        emit
    }

}