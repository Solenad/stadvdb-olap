from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "Users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(255))
    firstName: Mapped[str] = mapped_column(String(255))
    lastName: Mapped[str] = mapped_column(String(255))
    dateOfBirth: Mapped[Date] = mapped_column(Date)
    gender: Mapped[str] = mapped_column(String(255))
    
    sales: Mapped[list["FactSales"]] = relationship(back_populates="user")