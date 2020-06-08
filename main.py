import merlin.utils as ut


if __name__ == "__main__":
    print("initializing merlin..........")
    schema = ut.get_schema_for_env("dev")

    print(schema)