UNITS_NORMALIZATION = {
    "mj": u"megajoule",
    "kg": u"kilogram",
    "m3": u"cubic meter",
    "m2": u"square meter",
    'm3a': u"cubic meter-year",
    "m2a": u"square meter-year",
}

normalize_units = lambda x: UNITS_NORMALIZATION.get(x.lower(), x)
