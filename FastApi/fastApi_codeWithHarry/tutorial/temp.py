def add(firstName: str| list, lastName:str=None):
    firstName.capitalize()

    return firstName+" "+lastName
fname= "ujju"
lname="Rana"

name=add(fname,lname)
print(name)