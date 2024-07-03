import database_generator

if __name__ == "__main__":
    print("Generating data...")

    params = database_generator.DatabaseGeneratorParams(
        num_neighborhoods=15,
        num_users=1010,
        num_products=200,
        num_stores=5,
        qtd_stock_initial=200
    )

    sim = database_generator.DatabaseGenerator(params)
    sim.run()

    print("Data generated.")
