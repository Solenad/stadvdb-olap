from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import FactTable

class Base(DeclarativeBase):
    pass

class Product(Base):
    __tablename__ = "Products"
    FactSales = FactTable.FactSales

    id: Mapped[int] = mapped_column(primary_key=True)
    category: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(String(255))
    name: Mapped[str] = mapped_column(String(255))
    price: Mapped[float] = mapped_column(Float)

    sales: Mapped[list[FactSales]] = relationship(back_populates="product")