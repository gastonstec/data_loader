# Environment settings
class EnvSettings:
    env: str = "dev"
    db_connect: bool = True


# Application settings
class AppSettings:
    name: str = "DataLoader"
    version: str = "1.0.0"
    description: str = "Data Loader for GTIM"
    app_folder: str = "/Users/gastonsanchez/Downloads/dataloader"


# Database settings
class DBSettings:
    user: str = "postgres"
    password: str = "c4rec4"
    host: str = "localhost"
    port: int = 5432
    dbname: str = "gtim_services"
    appname: str = "DataLoader"
    min_size: int = 1
    max_size: int = 5
    timeout_qry: float = 5.0
    timeout_conn: float = 45.0

    def check_values(self) -> bool:
        # Validate the database settings.
        if (
            not self.user
            or not self.password
            or not self.host
            or not self.dbname
        ):
            return False
        # Validate the database port.
        if (
            not isinstance(self.port, int)
            or self.port < 1
        ):
            return False
        # Validate the database min_size.
        if (
            not isinstance(self.min_size, int)
            or self.min_size < 0
        ):
            return False
        # Validate the database max_size.
        if (
            not isinstance(self.max_size, int)
            or self.max_size <= 0
        ):
            return False
        # Validate the database min_size and max_size.
        if self.min_size > self.max_size:
            return False
        return True
