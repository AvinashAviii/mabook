from pydantic import BaseModel

class Note(BaseModel):
    title:str
    desc :str
    isImportant :bool = None
