 df_dados_lazy = df_dados.lazy()

    df_dados_lazy = (
        df_dados_lazy
        .group_by('produto')
        .agg((pl.col('quantidade') * pl.col('preco')).sum().alias('total'))
    )