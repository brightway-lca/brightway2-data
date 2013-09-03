UNITS_NORMALIZATION = {
    "mj": u"megajoule",
    "kg": u"kilogram",
    "m3": u"cubic meter",
    "m2": u"square meter",
    'm3a': u"cubic meter-year",
    "m2a": u"square meter-year",
    # SimaPro units to convert, ranging from sensible to bizarre
    "personkm": u"pkm",  # SimaPro changes this but doesn't change tkm!?
    "p": u"unit",
    "my": u"ma",  # SimaPro is much better (meter-year)
}

normalize_units = lambda x: UNITS_NORMALIZATION.get(x.lower(), x)
