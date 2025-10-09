from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import FactTable

class Base(DeclarativeBase):
    pass

class DateDimension(Base):
    __tablename__ = "Date"
    FactSales = FactTable.FactSales

    id: Mapped[int] = mapped_column(primary_key=True)
    createdAt: Mapped[Date] = mapped_column(Date)

    sales: Mapped[list[FactSales]] = relationship(back_populates="date")