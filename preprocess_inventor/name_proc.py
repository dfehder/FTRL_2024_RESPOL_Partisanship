"""
CREATE DATE: 02/01/2021
CREATED BY: DANIEL FEHDER
DESCRIPTION: various functions to aid the construction of the names
"""

def suffix(name):
    suffix_dict = {'jr':['jr', 'jr.', 'junior', 'jr. deceased', 'jr. ii', 'jr. iii', 'jr. executor'], 
                   'sr':['sr', 'sr.', 'senior'], 
                   'ii':['ii', 'ii.', '2nd'],
                   'iii':['iii', 'iii.', '3rd', '3'], 
                   'iv':['iv', 'iv.'], 
                   'v':['v', 'v.', '5th'],
                   'vi':['vi'],
                   'vii':['vii'], 'viii':['viii'], 'x':['x']}
    
    splits = name.split(',')
    if len(splits) > 1:
        s1 = splits[1].strip()
        for key, value in suffix_dict.items():
            if s1 in value:
                return key
            else:
                pass
    else:
        return ''


def middlei(name):
    splits = name.split(' ')
    if len(splits) > 1:
        return splits[-1][:1]
    else:
        return ''


# fist name clean
def name_1st(name):
    splits = name.split(' ')
    if len(splits) > 1:
        return splits[0]
    else:
        return name


# last name clean
def name_fam(name):
    splits = name.split(',')
    if len(splits) > 1:
        return splits[0]
    else:
        return name