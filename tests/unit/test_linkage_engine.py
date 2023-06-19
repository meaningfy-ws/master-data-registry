from master_data_registry.adapters.duckdb_adapter import DuckDBAdapter
from master_data_registry.adapters.linkage_engine import SplinkRecordLinkageEngine


def test_linkage_engine_dedupe_records(duplicates_records_dataframe, splink_model_config,
                                       unit_test_duckdb_path):
    """
    Test that the linkage engine deduplicates records.
    """
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(unit_test_duckdb_path)
    linkage_engine = SplinkRecordLinkageEngine(model_config=splink_model_config, duckdb_adapter=duckdb_adapter)
    duplicates_records_dataframe = linkage_engine.preprocess_data(data=duplicates_records_dataframe)
    linkage_engine.finetune_model_config(data=duplicates_records_dataframe, max_random_sampling_pairs=2)
    result_df = linkage_engine.dedupe_records(data=duplicates_records_dataframe,
                                              threshold_match_probability=0.8)
    assert len(result_df) == 2
    assert all(result_df['match_probability'] >= 0.8)
    unit_test_duckdb_path.unlink()


def test_linkage_engine_links_records(duplicates_records_dataframe, splink_model_config,
                                      unit_test_duckdb_path, reference_records_dataframe):
    """
    Test that the linkage engine links records.
    """
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(unit_test_duckdb_path)
    linkage_engine = SplinkRecordLinkageEngine(model_config=splink_model_config, duckdb_adapter=duckdb_adapter)
    duplicates_records_dataframe = linkage_engine.preprocess_data(data=duplicates_records_dataframe)
    linkage_engine.finetune_model_config(data=duplicates_records_dataframe, max_random_sampling_pairs=2)
    reference_table_name = 'reference_table'
    duckdb_adapter.create_table(reference_records_dataframe, reference_table_name)
    result_df = linkage_engine.link_records(data=duplicates_records_dataframe,
                                            reference_table_name=reference_table_name,
                                            threshold_match_probability=0.8)
    assert len(result_df) == 4
    assert all(result_df['match_probability'] >= 0.8)
    assert result_df["unique_id_r"].unique().tolist() == [1, 2]
    unit_test_duckdb_path.unlink()


def test_linkage_engine_dedupe_records_and_clustering(duplicates_records_dataframe, unit_test_duckdb_path,
                                                      splink_model_config):
    """
    Test that the linkage engine clusters deduplication result.
    """
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(unit_test_duckdb_path)
    linkage_engine = SplinkRecordLinkageEngine(model_config=splink_model_config, duckdb_adapter=duckdb_adapter)
    duplicates_records_dataframe = linkage_engine.preprocess_data(data=duplicates_records_dataframe)
    linkage_engine.finetune_model_config(data=duplicates_records_dataframe, max_random_sampling_pairs=2)
    result_df = linkage_engine.dedupe_records_and_clustering(data=duplicates_records_dataframe,
                                                             threshold_match_probability=0.8)
    assert "cluster_id" in result_df.columns
    assert len(result_df["cluster_id"].unique().tolist()) == 2
    unit_test_duckdb_path.unlink()
