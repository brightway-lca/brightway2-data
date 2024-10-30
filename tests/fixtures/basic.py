import os

biosphere = {
    ("biosphere", "1"): {
        "categories": ["things"],
        "code": 1,
        "exchanges": [],
        "name": "an emission",
        "type": "emission",
        "unit": "kg",
    },
    ("biosphere", "2"): {
        "categories": ["things"],
        "code": 2,
        "exchanges": [],
        "type": "emission",
        "name": "another emission",
        "unit": "kg",
    },
}

lcia = [(("biosphere", "1"), 10), (("biosphere", "2"), 1000)]

food = {
    ("food", "1"): {
        "categories": ["stuff", "meals"],
        "code": 1,
        "exchanges": [
            {
                "amount": 0.5,
                "input": ("food", "2"),
                "type": "technosphere",
                "uncertainty type": 0,
            },
            {
                "amount": 0.05,
                "input": ("biosphere", "1"),
                "type": "biosphere",
                "uncertainty type": 0,
            },
        ],
        "location": "CA",
        "name": "lunch",
        "type": "process",
        "unit": "kg",
    },
    ("food", "2"): {
        "categories": ["stuff", "meals"],
        "code": 2,
        "exchanges": [
            {
                "amount": 0.25,
                "input": ("food", "1"),
                "type": "technosphere",
                "uncertainty type": 0,
            },
            {
                "amount": 0.15,
                "input": ("biosphere", "2"),
                "type": "biosphere",
                "uncertainty type": 0,
            },
        ],
        "location": "CH",
        "name": "dinner",
        "type": "process",
        "unit": "kg",
    },
}

food2 = {
    ("food2", "1"): {
        "categories": ["stuff", "meals"],
        "code": 1,
        "exchanges": [
            {
                "amount": 0.5,
                "input": ("food", "2"),
                "type": "technosphere",
                "uncertainty type": 0,
            },
            {
                "amount": 0.05,
                "input": ("biosphere", "1"),
                "type": "biosphere",
                "uncertainty type": 0,
            },
        ],
        "location": "CA",
        "name": "lunch",
        "type": "process",
        "unit": "kg",
    },
    ("food2", "2"): {
        "categories": ["stuff", "meals"],
        "code": 2,
        "exchanges": [
            {
                "amount": 0.25,
                "input": ("food2", "1"),
                "type": "technosphere",
                "uncertainty type": 0,
            },
            {
                "amount": 0.15,
                "input": ("biosphere", "2"),
                "type": "biosphere",
                "uncertainty type": 0,
            },
        ],
        "location": "CH",
        "name": "dinner",
        "type": "process",
        "unit": "kg",
    },
}

get_naughty = lambda: [
    x.replace("\n", "")
    for x in open(os.path.join(os.path.dirname(__file__), "naughty_strings.txt"), encoding="utf8")
    if x[0] != "#"
]
