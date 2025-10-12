from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import FactTable

class Base(DeclarativeBase):
    pass

class Location(Base):
    __tablename__ = "Location"
    FactSales = FactTable.FactSales
    
    id: Mapped[int] = mapped_column(primary_key=True)
    address1: Mapped[str] = mapped_column(String(255))
    address2: Mapped[str | None] = mapped_column(String(255))
    city: Mapped[str] = mapped_column(String(255))
    country: Mapped[str] = mapped_column(String(255))
    zipCode: Mapped[str] = mapped_column(String(255))

    sales: Mapped[list[FactSales]] = relationship(back_populates="location")