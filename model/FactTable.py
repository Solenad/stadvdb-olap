from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import UserDimension
import LocationDimension
import ProductDimension
import DateDimension

class Base(DeclarativeBase):
    pass

class FactSales(Base):
    __tablename__ = "FactSales"
    User = UserDimension.User
    Location = LocationDimension.Location
    Product = ProductDimension.Product
    _Date = DateDimension.DateDimension

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    quantity: Mapped[int] = mapped_column(Integer)
    revenue: Mapped[float] = mapped_column(Float)
    OrderNumber: Mapped[str] = mapped_column(String(255))
    
    UserId: Mapped[int] = mapped_column(ForeignKey("Users.id"))
    ProductId: Mapped[int] = mapped_column(ForeignKey("Products.id"))
    LocationId: Mapped[int] = mapped_column(ForeignKey("Location.id"))
    DateId: Mapped[int] = mapped_column(ForeignKey("Date.id"))
    
    user: Mapped[User] = relationship(back_populates="sales")
    product: Mapped[Product] = relationship(back_populates="sales")
    location: Mapped[Location] = relationship(back_populates="sales")
    date: Mapped[_Date] = relationship(back_populates="sales")