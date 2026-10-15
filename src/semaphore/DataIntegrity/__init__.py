# Intentionally empty: import from the module itself, e.g.
# `from semaphore.DataIngestion.IDataIngestion import data_ingestion_factory`.
# A `*` re-export here exposed every name the module imported (ABC, Series, ...),
# made `SeriesProvider.SeriesProvider` mean the class instead of the module,
# and ran extra imports whenever anything in this package was loaded.
