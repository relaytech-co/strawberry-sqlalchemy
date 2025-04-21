import pytest
import strawberry
from sqlalchemy import Column, ForeignKey, Integer, String, Table, select
from sqlalchemy.orm import relationship
from strawberry_sqlalchemy_mapper import StrawberrySQLAlchemyLoader
from strawberry_sqlalchemy_mapper.mapper import StrawberrySQLAlchemyMapper


@pytest.fixture
def many_to_one_tables(base):
    class Employee(base):
        __tablename__ = "employee"
        id = Column(Integer, autoincrement=True, primary_key=True)
        name = Column(String, nullable=False)
        department_id = Column(Integer, ForeignKey("department.id"))
        __mapper_args__ = {"eager_defaults": True}

    class Department(base):
        __tablename__ = "department"
        id = Column(Integer, autoincrement=True, primary_key=True)
        name = Column(String, nullable=False)
        employees = relationship(
            "Employee",
            order_by="Employee.name",
        )
        __mapper_args__ = {"eager_defaults": True}

    return Employee, Department


@pytest.fixture
def secondary_tables(base):
    EmployeeDepartmentJoinTable = Table(
        "employee_department_join_table",
        base.metadata,
        Column("employee_id", ForeignKey("employee.e_id"), primary_key=True),
        Column("department_id", ForeignKey("department.d_id"), primary_key=True),
    )

    class Employee(base):
        __tablename__ = "employee"
        e_id = Column(Integer, autoincrement=True, primary_key=True)
        name = Column(String, nullable=False)
        departments = relationship(
            "Department",
            secondary="employee_department_join_table",
            back_populates="employees",
            order_by="Department.name",
        )

    class Department(base):
        __tablename__ = "department"
        d_id = Column(Integer, autoincrement=True, primary_key=True)
        name = Column(String, nullable=False)
        employees = relationship(
            "Employee",
            secondary="employee_department_join_table",
            back_populates="departments",
            order_by="Employee.name",
        )

    return Employee, Department


@pytest.fixture
def mapper():
    return StrawberrySQLAlchemyMapper()


def _generate_schema(employee_model, department_model, mapper):
    @mapper.type(employee_model)
    class Employee:
        __exclude__ = ["password_hash"]

    @mapper.type(department_model)
    class Department:
        pass

    @strawberry.type
    class Query:
        @strawberry.field
        def departments(self, info: strawberry.Info) -> list[Department]:
            return (
                info.context["session"]
                .execute(select(department_model))
                .scalars()
                .all()
            )

    return strawberry.Schema(Query)


@pytest.fixture
def many_to_one_schema(many_to_one_tables, mapper):
    EmployeeModel, DepartmentModel = many_to_one_tables
    return _generate_schema(EmployeeModel, DepartmentModel, mapper)


@pytest.fixture
def secondary_schema(secondary_tables, mapper):
    EmployeeModel, DepartmentModel = secondary_tables
    return _generate_schema(EmployeeModel, DepartmentModel, mapper)


@pytest.mark.asyncio
async def test_many_to_one_tables(
    many_to_one_tables, many_to_one_schema, sessionmaker, engine, base
):
    EmployeeModel, DepartmentModel = many_to_one_tables
    base.metadata.create_all(engine)

    with sessionmaker() as session:
        e1 = EmployeeModel(name="e1")
        e2 = EmployeeModel(name="e2")
        d1 = DepartmentModel(name="d1")
        d2 = DepartmentModel(name="d2")
        session.add(e1)
        session.add(e2)
        session.add(d1)
        session.add(d2)
        session.flush()

        d1.employees.append(e2)
        d2.employees.append(e1)
        session.commit()

        result = await many_to_one_schema.execute(
            """
            query {
                departments {
                    name

                    employees {
                        edges {
                            node {
                                name
                            }
                        }
                    }
                }
            }
            """,
            context_value={
                "session": session,
                "sqlalchemy_loader": StrawberrySQLAlchemyLoader(bind=session),
            },
        )

        assert result.data == {
            "departments": [
                {
                    "name": "d1",
                    "employees": {"edges": [{"node": {"name": "e2"}}]},
                },
                {
                    "name": "d2",
                    "employees": {"edges": [{"node": {"name": "e1"}}]},
                },
            ]
        }


@pytest.mark.asyncio
async def test_secondary_tables(
    secondary_tables, secondary_schema, sessionmaker, engine, base
):
    EmployeeModel, DepartmentModel = secondary_tables
    base.metadata.create_all(engine)

    with sessionmaker() as session:
        e1 = EmployeeModel(name="e1")
        e2 = EmployeeModel(name="e2")
        d1 = DepartmentModel(name="d1")
        d2 = DepartmentModel(name="d2")
        session.add(e1)
        session.add(e2)
        session.add(d1)
        session.add(d2)
        session.flush()

        e1.departments.append(d1)
        e1.departments.append(d2)
        e2.departments.append(d2)
        session.commit()

        result = await secondary_schema.execute(
            """
            query {
                departments {
                    name

                    employees {
                        edges {
                            node {
                                name
                            }
                        }
                    }
                }
            }
            """,
            context_value={
                "session": session,
                "sqlalchemy_loader": StrawberrySQLAlchemyLoader(bind=session),
            },
        )

        assert result.data == {
            "departments": [
                {
                    "name": "d1",
                    "employees": {"edges": [{"node": {"name": "e1"}}]},
                },
                {
                    "name": "d2",
                    "employees": {
                        "edges": [{"node": {"name": "e1"}}, {"node": {"name": "e2"}}]
                    },
                },
            ]
        }
