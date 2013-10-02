UNITS_NORMALIZATION = {
    "ha": u"hectare",
    "kbq": u"kilo Becquerel",
    "kg": u"kilogram",
    "km": u"kilometer",
    "kwh": u"kilowatt hour",
    "m2": u"square meter",
    "m2a": u"square meter-year",
    "m3": u"cubic meter",
    "ma": u"meter-year",
    "mj": u"megajoule",
    "nm3": u"cubic meter",
    "pkm": u"person kilometer",
    "tkm": u"ton kilometer",
    "vkm": u"vehicle kilometer",
    # SimaPro units to convert, ranging from sensible to bizarre
    "m3a": u"cubic meter-year",
    "personkm": u"person kilometer",
    "p": u"unit",
    "my": u"meter-year",
}

normalize_units = lambda x: UNITS_NORMALIZATION.get(x.lower(), x)



