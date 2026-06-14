def noteEntity(item) -> dict:
    return {
        "id":str(item["id"]),
        "title":item["title"],
        "desc":item["desc"],
        "isImportant":item["isImportant"],
    }

def notesEntity(items) ->list:
    return [noteEntity(item) for item in items]
