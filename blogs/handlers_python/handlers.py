#!/usr/bin/python3
# 3 columns: id, name, extra
data=(
    (1, "John"),
    (2, "Amanda"),
    (3, "Tom"),
    (4, "Karen"),
    (5, "Nathan"),
    (6, "Lisa"),
    (7, "Johnathan"),
    (8, "Yvette"),
    (9, "John"), # 2nd John
)

# name sorted alphabetically, associated with their primary key
name_index = (
    ("Amanda", [2]),
    ("John", [1,9]), # there are 2 primary keys matching this name
    ("Johnathan", [7]),
    ("Karen", [4]),
    ("Lisa", [6]),
    ("Nathan", [5]),
    ("Tom", [3]),
    ("Yvette", [8]),
)



# CREATE TABLE data (id integer auto_increment primary key, name varchar(60), extra varchar(60));
# CREATE INDEX name_index ON data(name);
# INSERT INTO data (name) VALUES ('John'), ('Amanda'), ('Tom'), ('Karen'), ('Nathan'), ('Lisa'), ('Johnathan'), ('Yvette'), ('John');

id_col=0
name_col=1
extra_col=2

def reset_handlers():
    global handler_read_first, handler_read_key, handler_read_next, handler_read_rnd_next, special_demo_handler
    handler_read_first=0
    handler_read_key=0
    handler_read_next=0
    handler_read_rnd_next=0
    special_demo_handler=0
reset_handlers()

def print_handlers():
    print('handler_read_first: %d, handler_read_key: %d, handler_read_next: %d, handler_read_rnd_next: %d, special_demo_handler: %d' % (handler_read_first, handler_read_key, handler_read_next, handler_read_rnd_next, special_demo_handler))

def print_results(out):
    print(out)
    print("row count:", len(out))
    print_handlers()
    reset_handlers()

def search_using_index(idx, to_find, is_prefix_search=False, increment_handlers=True):
    global handler_read_key, special_demo_handler
    if increment_handlers:
        handler_read_key = handler_read_key +1
    # we first consider the whole index
    low = 0
    high = len(data) - 1

    # it is a simple "binary search" on a list
    while low <= high:
        # We take the value right at the middle between "low" and "high"
        middle = low + (high - low) // 2
        row_indexed_value = idx[middle][0]
        special_demo_handler = special_demo_handler + 1
        
        if is_prefix_search and len(row_indexed_value) > len(to_find):
            row_indexed_value = row_indexed_value[:len(to_find)]

        if row_indexed_value == to_find:
            return middle
        elif row_indexed_value < to_find:
            # we search something higher, so we will only consider the half with higher values
            low = middle + 1
        else:
            high = middle - 1
    return -1


print("\nSearching data using primary key:")
print("SELECT * FROM table WHERE id=7;")
# "data" is the table, but it's also the primary key, hence an index
print_results([ data[search_using_index(data, 7)] ])

# could use it that way, but it won't make much sense for secondary_row
print_results([ name_index[search_using_index(name_index, "Yvette")] ])


def search_using_secondary_index(table, index, to_search):
    global handler_read_next, special_demo_handler

    # we get the row from index, actually re-using the same index search
    secondary_row=index[search_using_index(index, to_search)]
    results=[]

    # then mysql gets the actual data row from primary keys, using the associated ids
    for id in secondary_row[1]:
        handler_read_next = handler_read_next + 1
        special_demo_handler = special_demo_handler + 1

        results.append(table[search_using_index(table, id, False, False)])
    return results

# Actually the same function we used for id. It will increment the same handlers
print("\nSearching data using name, indexed:")
print("SELECT * FROM table WHERE name ='John';")
print_results(search_using_secondary_index(data, name_index, "John"))




def search_range(idx, range_to_find, is_prefix_search=False, increment_handlers=True):
    global handler_read_first, handler_read_next, special_demo_handler

    results=[]

    # we start from an index search of the lowest element of the range
    start = search_using_index(idx, range_to_find[0], is_prefix_search)
   
    # then iterate on each next row to add it to the results
    for row_idx in range(start, len(idx)):
        if row_idx == 0 and increment_handlers: # if we had to start from the very 1st element of the index
            handler_read_first = handler_read_first + 1
            
        val = idx[row_idx][0]
        special_demo_handler = special_demo_handler + 1

        if len(str(range_to_find[0])) < len(str(val)):
            val=val[:len(str(range_to_find[0]))]    
            
        if val < range_to_find[1]:
            if increment_handlers:
                handler_read_next = handler_read_next + 1
            results.append(idx[row_idx])
        else:
            break
    return results

print("\nSearching data using range on primary key, starting from beginning of the table:")
print("SELECT * FROM table WHERE id < 5;")
print_results(search_range(data, (1, 5))) # we set 1 as it's the minimal id here 

# We could make something similar with handler_read_last and handler_read_prev
# It would work for: SELECT * FROM table WHERE id > 5 ORDER BY id DESC;
# It would start from the end of the table and iterate backward, but the idea is identical

print("\nSearching data using range on primary key:")
print("SELECT * FROM table WHERE id BETWEEN 2 AND 5;")
print_results(search_range(data, (2, 5)))



def search_using_secondary_range(table, index, range_to_find, is_prefix_search=False):
    global handler_read_next, special_demo_handler

    # we get the row from index, actually re-using the same index search
    secondary_rows=search_range(index, range_to_find, is_prefix_search, False)
    results=[]

    # then mysql gets the actual data row from primary keys, using the associated ids
    for idx_row in secondary_rows:
        for id in idx_row[1]:
            handler_read_next=handler_read_next+1
            special_demo_handler = special_demo_handler + 1

            results.append(table[search_using_index(table, id, False, False)])
            #results.append((id, idx_row[0]))
    return results

print("\nSearching data using name prefix:")
print("SELECT * FROM table WHERE name like 'Joh%';")
print_results(search_using_secondary_range(data, name_index, ("Joh", "Joi"), True))


print("\nSearching data using name prefix:")
print("SELECT * FROM table WHERE name > 'Joh%' and name < 'Yve%';")
print_results(search_using_secondary_range(data, name_index, ("Joh", "Yve"), True))


def search_name_suffix(data, suffix: str):
    global handler_read_rnd_next, handler_read_first, handler_read_key, special_demo_handler
    results=[]
    # remove the %
    suffix=suffix[1:len(suffix)]

    handler_read_key = handler_read_key + 1

    for row_idx in range(0, len(data)):
        if row_idx == 0: # if we had to start from the very 1st element of the index
            handler_read_first = handler_read_first + 1
        handler_read_rnd_next = handler_read_rnd_next + 1
        name = data[row_idx][1]
        special_demo_handler = special_demo_handler + 1
        # check if the end is identical
        if name[len(name)-len(suffix):len(name)] == suffix:
            results.append(data[row_idx])
    return results

print("\nSearching data using name suffix:")
print("SELECT * FROM table WHERE name like '%athan';")
print_results(search_name_suffix(data, "%athan"))







data= (
    (0, "LINWOOD"), (1, "KEIRA"), (2, "KELVIN"), (3, "KARLA"), (4, "CALVIN"), (5, "LATICIA"), (6, "KRIS"), (7, "SUNG"), (8, "ELMO"), (9, "LOVETTA"), (10, "LAURENCE"), 
    (11, "MARCI"), (12, "DAREN"), (13, "NINFA"), (14, "ERASMO"), (15, "HERTA"), (16, "DARON"), (17, "LYNDSEY"), (18, "MATTHEW"), (19, "SANTANA"), (20, "CHASE"), 
    (21, "ZAIDA"), (22, "DONOVAN"), (23, "PATRINA"), (24, "TAD"), (25, "PAM"), (26, "DWAYNE"), (27, "ROSEANN"), (28, "LELAND"), (29, "LORIA"), (30, "FREEMAN"), 
    (31, "GWYN"), (32, "JAY"), (33, "LORIANN"), (34, "DUNCAN"), (35, "MAPLE"), (36, "CLETUS"), (37, "SOILA"), (38, "TERRENCE"), (39, "MARIAH"), (40, "JAME"), 
    (41, "GENEVIVE"), (42, "MYLES"), (43, "AUSTIN"), (44, "CYRUS"), (45, "CLORINDA"), (46, "WINSTON"), (47, "TAWNYA"), (48, "NORBERT"), (49, "BRITTANY"), (50, "JAN"), 
    (51, "KATI"), (52, "EZEQUIEL"), (53, "SAUNDRA"), (54, "JERAMY"), (55, "DIANNE"), (56, "SANG"), (57, "CAMI"), (58, "BRENTON"), (59, "KATRINA"), (60, "AARON"), 
    (61, "ORETHA"), (62, "DERICK"), (63, "BRITT"), (64, "GONZALO"), (65, "HILDA"), (66, "MITCH"), (67, "SOLEDAD"), (68, "RANDELL"), (69, "ISIS"), (70, "MERLIN"), 
    (71, "EWA"), (72, "THOMAS"), (73, "TOMASA"), (74, "GEOFFREY"), (75, "LAURENCE"), (76, "EZEKIEL"), (77, "KHADIJAH"), (78, "FRANKIE"), (79, "JANICE"), (80, "ALLAN"), 
    (81, "ODILIA"), (82, "MANUEL"), (83, "NAOMA"), (84, "MACK"), (85, "BRITTNY"), (86, "MARSHALL"), (87, "TARAH"), (88, "BRUNO"), (89, "INEZ"), (90, "RODERICK"), 
    (91, "CLYDE"), (92, "SEAN"), (93, "JANEAN"), (94, "KENT"), (95, "KARISA"), (96, "SHERWOOD"), (97, "ANGELINA"), (98, "TRACY"), (99, "JORDAN"), (100, "GENE"), 
    (101, "LATASHIA"), (102, "LACY"), (103, "ORALIA"), (104, "FOSTER"), (105, "NATALYA"), (106, "CLIFFORD"), (107, "GEARLDINE"), (108, "GREGORIO"), (109, "CAMILLE"), (110, "RICKIE"), 
    (111, "MARLANA"), (112, "JULIAN"), (113, "GLYNIS"), (114, "ROLLAND"), (115, "ADELAIDE"), (116, "CORNELIUS"), (117, "BEULAH"), (118, "BRODERICK"), (119, "MARGENE"), (120, "WILFREDO"), 
    (121, "SUNDAY"), (122, "MARCOS"), (123, "MARIAM"), (124, "RICK"), (125, "MARX"), (126, "ARMANDO"), (127, "DANIKA"), (128, "THEODORE"), (129, "AMIE"), (130, "SHAD"), 
    (131, "FREDRICKA"), (132, "MILES"), (133, "CLEORA"), (134, "DOYLE"), (135, "JOI"), (136, "HARLAN"), (137, "ALIX"), (138, "DANE"), (139, "MONNIE"), (140, "IGNACIO"), 
    (141, "DEBRAH"), (142, "GALEN"), (143, "KELSI"), (144, "GERARDO"), (145, "REDA"), (146, "JOHN"), (147, "ALLENA"), (148, "JOSEF"), (149, "SARITA"), (150, "ALVIN"), 
    (151, "LINA"), (152, "NOE"), (153, "KRYSTEN"), (154, "QUINTIN"), (155, "SHANTELL"), (156, "HASSAN"), (157, "RAYE"), (158, "JAKE"), (159, "BERNIECE"), (160, "TERRY"), 
    (161, "LEONORE"), (162, "CLAUDIO"), (163, "JANEAN"), (164, "LESTER"), (165, "DENA"), (166, "RUDY"), (167, "SHARON"), (168, "PHILIP"), (169, "GABRIELA"), (170, "THEO"), 
    (171, "MAUDE"), (172, "DOMINICK"), (173, "DEBRAH"), (174, "JAVIER"), (175, "WENDY"), (176, "ALPHONSO"), (177, "GRACE"), (178, "ANDRE"), (179, "ALANNA"), (180, "DION"), 
    (181, "AUGUSTA"), (182, "EDGAR"), (183, "CECILY"), (184, "ALEX"), (185, "CAROLYN"), (186, "MICHEAL"), (187, "GALE"), (188, "HUBERT"), (189, "ELLA"), (190, "NEIL"), 
    (191, "INGA"), (192, "OLLIE"), (193, "ETHA"), (194, "JERRELL"), (195, "FELICE"), (196, "CLETUS"), (197, "EMMY"), (198, "MARY"), (199, "ALBA"), (200, "VAL"), 
    (201, "TASHIA"), (202, "AMBROSE"), (203, "KANDY"), (204, "FELTON"), (205, "VALDA"), (206, "TORY"), (207, "ARLINE"), (208, "OTTO"), (209, "ULRIKE"), (210, "GEORGE"), 
    (211, "SARI"), (212, "EMILE"), (213, "PEGGY"), (214, "FELIPE"), (215, "NORMAN"), (216, "TYRELL"), (217, "BULAH"), (218, "BURTON"), (219, "REBA"), (220, "GAYLORD"), 
    (221, "JANICE"), (222, "MERLE"), (223, "ALISSA"), (224, "OSVALDO"), (225, "KAZUKO"), (226, "HIRAM"), (227, "TALIA"), (228, "SHAYNE"), (229, "KATRINA"), (230, "DWIGHT"), 
    (231, "MARYLAND"), (232, "SAUL"), (233, "LOURA"), (234, "RICKIE"), (235, "MATTHEW"), (236, "QUINN"), (237, "SOPHIA"), (238, "HARLEY"), (239, "ZOLA"), (240, "JONAH"), 
    (241, "SCARLETT"), (242, "SIMON"), (243, "LETTY"), (244, "SAL"), (245, "CATINA"), (246, "BURT"), (247, "PAMALA"), (248, "DONN"), (249, "LILLIAN"), (250, "JAMISON"), 
    (251, "KIRSTIE"), (252, "STERLING"), (253, "ZELLA"), (254, "ALPHONSO"), (255, "ALLYN"), (256, "FOSTER"), (257, "WINIFRED"), (258, "JEWELL"), (259, "ANISA"), (260, "JED"), 
    (261, "ANGLE"), (262, "LAWRENCE"), (263, "MAREN"), (264, "KEVEN"), (265, "LONI"), (266, "COLUMBUS"), (267, "BILLY"), (268, "STACY"), (269, "ALESHA"), (270, "JOSE"), 
    (271, "SHARYL"), (272, "SHAWN"), (273, "AILENE"), (274, "JOSH"), (275, "EVAN"), (276, "LOYD"), (277, "CORINA"), (278, "STEWART"), (279, "TONDA"), (280, "MALCOM"), 
    (281, "MAJORIE"), (282, "DUSTY"), (283, "ANNIS"), (284, "LOGAN"), (285, "BENITA"), (286, "EMANUEL"), (287, "JULE"), (288, "BRAD"), (289, "LATOYA"), (290, "CHONG"), 
    (291, "SUZANN"), (292, "ALLEN"), (293, "SHANTEL"), (294, "ANDREAS"), (295, "SONDRA"), (296, "TRAVIS"), (297, "TAUNYA"), (298, "BARTON"), (299, "MONET"), (300, "ED"), 
    (301, "EDITH"), (302, "KURTIS"), (303, "NEELY"), (304, "RON"), (305, "TERRY"), (306, "JULIO"), (307, "AUDRY"), (308, "MITCHEL"), (309, "JOHNETTA"), (310, "RAYFORD"), 
    (311, "DEIRDRE"), (312, "JESS"), (313, "PHEBE"), (314, "ANTOINE"), (315, "BETHANIE"), (316, "FILIBERTO"), (317, "CHARISE"), (318, "JOHNATHAN"), (319, "GERRY"), (320, "CLIFTON"), 
    (321, "MARIELLE"), (322, "REX"), (323, "CATRICE"), (324, "ODELL"), (325, "KIRSTEN"), (326, "WERNER"), (327, "ERA"), (328, "LAUREN"), (329, "TAMAR"), (330, "SIMON"), 
    (331, "LECIA"), (332, "DICK"), (333, "YUN"), (334, "NICOLAS"), (335, "YER"), (336, "KRISTOPHER"), (337, "BERNIECE"), (338, "GREGG"), (339, "ELSIE"), (340, "KING"), 
    (341, "EVELYNN"), (342, "FREDRICK"), (343, "LAUREL"), (344, "LEON"), (345, "MABELLE"), (346, "SOL"), (347, "ESTELLA"), (348, "HAYDEN"), (349, "LAVERNA"), (350, "STACEY"), 
    (351, "PAOLA"), (352, "STEWART"), (353, "MAYRA"), (354, "ELWOOD"), (355, "SHEMEKA"), (356, "LESLIE"), (357, "DIANNE"), (358, "GARRET"), (359, "ROSALYN"), (360, "JESSE"), 
    (361, "OUIDA"), (362, "FELTON"), (363, "SHERITA"), (364, "JUAN"), (365, "PENNEY"), (366, "ELLSWORTH"), (367, "WEI"), (368, "LEIGH"), (369, "LAURINE"), (370, "FRANKLIN"), 
    (371, "ILEANA"), (372, "BEN"), (373, "DENISHA"), (374, "CARMELO"), (375, "JANIE"), (376, "GASTON"), (377, "ROSALIA"), (378, "CLIFFORD"), (379, "LORRIANE"), (380, "HEATH"), 
    (381, "PAOLA"), (382, "MARLON"), (383, "BIANCA"), (384, "CHESTER"), (385, "EVELYN"), (386, "STEVEN"), (387, "ANGELINE"), (388, "WILBURN"), (389, "SACHIKO"), (390, "IRWIN"), 
    (391, "KELSEY"), (392, "PASQUALE"), (393, "LATOYIA"), (394, "ALEC"), (395, "BUFFY"), (396, "SERGIO"), (397, "NOEMI"), (398, "SOLOMON"), (399, "PAT")
)

name_index=(
    ("AARON", [60]), ("ADELAIDE", [115]), ("AILENE", [273]), ("ALANNA", [179]), ("ALBA", [199]), ("ALEC", [394]), ("ALESHA", [269]), ("ALEX", [184]), ("ALISSA", [223]),("ALIX", [137]), ("ALLAN", [80]), 
    ("ALLEN", [292]), ("ALLENA", [147]), ("ALLYN", [255]), ("ALPHONSO", [176,254]), ("ALPHONSO", [176,254]), ("ALVIN", [150]), ("AMBROSE", [202]), ("AMIE", [129]), ("ANDRE", [178]), ("ANDREAS", [294]), 
    ("ANGELINA", [97]), ("ANGELINE", [387]), ("ANGLE", [261]), ("ANISA", [259]), ("ANNIS", [283]), ("ANTOINE", [314]), ("ARLINE", [207]), ("ARMANDO", [126]), ("AUDRY", [307]), ("AUGUSTA", [181]), 
    ("AUSTIN", [43]), ("BARTON", [298]), ("BEN", [372]), ("BENITA", [285]), ("BERNIECE", [159,337]), ("BERNIECE", [159,337]), ("BETHANIE", [315]), ("BEULAH", [117]), ("BIANCA", [383]), ("BILLY", [267]), 
    ("BRAD", [288]), ("BRENTON", [58]), ("BRITT", [63]), ("BRITTANY", [49]), ("BRITTNY", [85]), ("BRODERICK", [118]), ("BRUNO", [88]), ("BUFFY", [395]), ("BULAH", [217]), ("BURT", [246]), 
    ("BURTON", [218]), ("CALVIN", [4]), ("CAMI", [57]), ("CAMILLE", [109]), ("CARMELO", [374]), ("CAROLYN", [185]), ("CATINA", [245]), ("CATRICE", [323]), ("CECILY", [183]), ("CHARISE", [317]), 
    ("CHASE", [20]), ("CHESTER", [384]), ("CHONG", [290]), ("CLAUDIO", [162]), ("CLEORA", [133]), ("CLETUS", [36,196]), ("CLETUS", [36,196]), ("CLIFFORD", [106,378]), ("CLIFFORD", [106,378]), ("CLIFTON", [320]), 
    ("CLORINDA", [45]), ("CLYDE", [91]), ("COLUMBUS", [266]), ("CORINA", [277]), ("CORNELIUS", [116]), ("CYRUS", [44]), ("DANE", [138]), ("DANIKA", [127]), ("DAREN", [12]), ("DARON", [16]), 
    ("DEBRAH", [141,173]), ("DEBRAH", [141,173]), ("DEIRDRE", [311]), ("DENA", [165]), ("DENISHA", [373]), ("DERICK", [62]), ("DIANNE", [55,357]), ("DIANNE", [55,357]), ("DICK", [332]), ("DION", [180]), 
    ("DOMINICK", [172]), ("DONN", [248]), ("DONOVAN", [22]), ("DOYLE", [134]), ("DUNCAN", [34]), ("DUSTY", [282]), ("DWAYNE", [26]), ("DWIGHT", [230]), ("ED", [300]), ("EDGAR", [182]), 
    ("EDITH", [301]), ("ELLA", [189]), ("ELLSWORTH", [366]), ("ELMO", [8]), ("ELSIE", [339]), ("ELWOOD", [354]), ("EMANUEL", [286]), ("EMILE", [212]), ("EMMY", [197]), ("ERA", [327]), 
    ("ERASMO", [14]), ("ESTELLA", [347]), ("ETHA", [193]), ("EVAN", [275]), ("EVELYN", [385]), ("EVELYNN", [341]), ("EWA", [71]), ("EZEKIEL", [76]), ("EZEQUIEL", [52]), ("FELICE", [195]), 
    ("FELIPE", [214]), ("FELTON", [204,362]), ("FELTON", [204,362]), ("FILIBERTO", [316]), ("FOSTER", [104,256]), ("FOSTER", [104,256]), ("FRANKIE", [78]), ("FRANKLIN", [370]), ("FREDRICK", [342]), ("FREDRICKA", [131]), 
    ("FREEMAN", [30]), ("GABRIELA", [169]), ("GALE", [187]), ("GALEN", [142]), ("GARRET", [358]), ("GASTON", [376]), ("GAYLORD", [220]), ("GEARLDINE", [107]), ("GENE", [100]), ("GENEVIVE", [41]), 
    ("GEOFFREY", [74]), ("GEORGE", [210]), ("GERARDO", [144]), ("GERRY", [319]), ("GLYNIS", [113]), ("GONZALO", [64]), ("GRACE", [177]), ("GREGG", [338]), ("GREGORIO", [108]), ("GWYN", [31]), 
    ("HARLAN", [136]), ("HARLEY", [238]), ("HASSAN", [156]), ("HAYDEN", [348]), ("HEATH", [380]), ("HERTA", [15]), ("HILDA", [65]), ("HIRAM", [226]), ("HUBERT", [188]), ("IGNACIO", [140]), 
    ("ILEANA", [371]), ("INEZ", [89]), ("INGA", [191]), ("IRWIN", [390]), ("ISIS", [69]), ("JAKE", [158]), ("JAME", [40]), ("JAMISON", [250]), ("JAN", [50]), ("JANEAN", [93,163]), 
    ("JANEAN", [93,163]), ("JANICE", [79,221]), ("JANICE", [79,221]), ("JANIE", [375]), ("JAVIER", [174]), ("JAY", [32]), ("JED", [260]), ("JERAMY", [54]), ("JERRELL", [194]), ("JESS", [312]), 
    ("JESSE", [360]), ("JEWELL", [258]), ("JOHN", [146]), ("JOHNATHAN", [318]), ("JOHNETTA", [309]), ("JOI", [135]), ("JONAH", [240]), ("JORDAN", [99]), ("JOSE", [270]), ("JOSEF", [148]), 
    ("JOSH", [274]), ("JUAN", [364]), ("JULE", [287]), ("JULIAN", [112]), ("JULIO", [306]), ("KANDY", [203]), ("KARISA", [95]), ("KARLA", [3]), ("KATI", [51]), ("KATRINA", [59,229]), 
    ("KATRINA", [59,229]), ("KAZUKO", [225]), ("KEIRA", [1]), ("KELSEY", [391]), ("KELSI", [143]), ("KELVIN", [2]), ("KENT", [94]), ("KEVEN", [264]), ("KHADIJAH", [77]), ("KING", [340]), 
    ("KIRSTEN", [325]), ("KIRSTIE", [251]), ("KRIS", [6]), ("KRISTOPHER", [336]), ("KRYSTEN", [153]), ("KURTIS", [302]), ("LACY", [102]), ("LATASHIA", [101]), ("LATICIA", [5]), ("LATOYA", [289]), 
    ("LATOYIA", [393]), ("LAUREL", [343]), ("LAUREN", [328]), ("LAURENCE", [10,75]), ("LAURENCE", [10,75]), ("LAURINE", [369]), ("LAVERNA", [349]), ("LAWRENCE", [262]), ("LECIA", [331]), ("LEIGH", [368]), 
    ("LELAND", [28]), ("LEON", [344]), ("LEONORE", [161]), ("LESLIE", [356]), ("LESTER", [164]), ("LETTY", [243]), ("LILLIAN", [249]), ("LINA", [151]), ("LINWOOD", [0]), ("LOGAN", [284]), 
    ("LONI", [265]), ("LORIA", [29]), ("LORIANN", [33]), ("LORRIANE", [379]), ("LOURA", [233]), ("LOVETTA", [9]), ("LOYD", [276]), ("LYNDSEY", [17]), ("MABELLE", [345]), ("MACK", [84]), 
    ("MAJORIE", [281]), ("MALCOM", [280]), ("MANUEL", [82]), ("MAPLE", [35]), ("MARCI", [11]), ("MARCOS", [122]), ("MAREN", [263]), ("MARGENE", [119]), ("MARIAH", [39]), ("MARIAM", [123]), 
    ("MARIELLE", [321]), ("MARLANA", [111]), ("MARLON", [382]), ("MARSHALL", [86]), ("MARX", [125]), ("MARY", [198]), ("MARYLAND", [231]), ("MATTHEW", [18,235]), ("MATTHEW", [18,235]), ("MAUDE", [171]), 
    ("MAYRA", [353]), ("MERLE", [222]), ("MERLIN", [70]), ("MICHEAL", [186]), ("MILES", [132]), ("MITCH", [66]), ("MITCHEL", [308]), ("MONET", [299]), ("MONNIE", [139]), ("MYLES", [42]), 
    ("NAOMA", [83]), ("NATALYA", [105]), ("NEELY", [303]), ("NEIL", [190]), ("NICOLAS", [334]), ("NINFA", [13]), ("NOE", [152]), ("NOEMI", [397]), ("NORBERT", [48]), ("NORMAN", [215]), 
    ("ODELL", [324]), ("ODILIA", [81]), ("OLLIE", [192]), ("ORALIA", [103]), ("ORETHA", [61]), ("OSVALDO", [224]), ("OTTO", [208]), ("OUIDA", [361]), ("PAM", [25]), ("PAMALA", [247]), 
    ("PAOLA", [351,381]), ("PAOLA", [351,381]), ("PASQUALE", [392]), ("PAT", [399]), ("PATRINA", [23]), ("PEGGY", [213]), ("PENNEY", [365]), ("PHEBE", [313]), ("PHILIP", [168]), ("QUINN", [236]), 
    ("QUINTIN", [154]), ("RANDELL", [68]), ("RAYE", [157]), ("RAYFORD", [310]), ("REBA", [219]), ("REDA", [145]), ("REX", [322]), ("RICK", [124]), ("RICKIE", [110,234]), ("RICKIE", [110,234]), 
    ("RODERICK", [90]), ("ROLLAND", [114]), ("RON", [304]), ("ROSALIA", [377]), ("ROSALYN", [359]), ("ROSEANN", [27]), ("RUDY", [166]), ("SACHIKO", [389]), ("SAL", [244]), ("SANG", [56]), 
    ("SANTANA", [19]), ("SARI", [211]), ("SARITA", [149]), ("SAUL", [232]), ("SAUNDRA", [53]), ("SCARLETT", [241]), ("SEAN", [92]), ("SERGIO", [396]), ("SHAD", [130]), ("SHANTEL", [293]), 
    ("SHANTELL", [155]), ("SHARON", [167]), ("SHARYL", [271]), ("SHAWN", [272]), ("SHAYNE", [228]), ("SHEMEKA", [355]), ("SHERITA", [363]), ("SHERWOOD", [96]), ("SIMON", [242,330]), ("SIMON", [242,330]), 
    ("SOILA", [37]), ("SOL", [346]), ("SOLEDAD", [67]), ("SOLOMON", [398]), ("SONDRA", [295]), ("SOPHIA", [237]), ("STACEY", [350]), ("STACY", [268]), ("STERLING", [252]), ("STEVEN", [386]), 
    ("STEWART", [278,352]), ("STEWART", [278,352]), ("SUNDAY", [121]), ("SUNG", [7]), ("SUZANN", [291]), ("TAD", [24]), ("TALIA", [227]), ("TAMAR", [329]), ("TARAH", [87]), ("TASHIA", [201]), 
    ("TAUNYA", [297]), ("TAWNYA", [47]), ("TERRENCE", [38]), ("TERRY", [160,305]), ("TERRY", [160,305]), ("THEO", [170]), ("THEODORE", [128]), ("THOMAS", [72]), ("TOMASA", [73]), ("TONDA", [279]), 
    ("TORY", [206]), ("TRACY", [98]), ("TRAVIS", [296]), ("TYRELL", [216]), ("ULRIKE", [209]), ("VAL", [200]), ("VALDA", [205]), ("WEI", [367]), ("WENDY", [175]), ("WERNER", [326]), 
    ("WILBURN", [388]), ("WILFREDO", [120]), ("WINIFRED", [257]), ("WINSTON", [46]), ("YER", [335]), ("YUN", [333]), ("ZAIDA", [21]), ("ZELLA", [253]), ("ZOLA", [239])
)

print("\nSearching data using primary key:")
print("SELECT * FROM table WHERE id=235;")
print_results([data[search_using_index(data, 235)]])


print("\nSearching data using range on primary key:")
print("SELECT * FROM table WHERE id BETWEEN 214 AND 227;")
print_results(search_range(data, (214, 227)))



print("\nSearching data using name, indexed:")
print("SELECT * FROM table WHERE name ='GLYNIS';")
print_results(search_using_secondary_index(data, name_index, "GLYNIS"))

print("\nSearching data using name prefix:")
print("SELECT * FROM table WHERE name like 'MAR%';")
print_results(search_using_secondary_range(data, name_index, ("MAR", "MAS"), True))


print("\nSearching data using name prefix:")
print("SELECT * FROM table WHERE name > 'KAT%' and name < 'LAU%';")
print_results(search_using_secondary_range(data, name_index, ("KAT", "LAU"), True))

print("\nSearching data using name suffix:")
print("SELECT * FROM table WHERE name like '%RA';")
print_results(search_name_suffix(data, "%RA"))
