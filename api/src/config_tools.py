def find_item_by_name(nm, ls):
    return list(filter(lambda i: i['name']==nm, ls))[0]

def append_model_name(model_nm, ls):
    return [{'model_name': model_nm, **i} for i in ls]
