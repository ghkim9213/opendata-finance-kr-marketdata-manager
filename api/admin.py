from .models import *
from django.contrib import admin

@admin.register(SingleAccountClient)
class SingleAccountClientAdmin(admin.ModelAdmin):
    pass

@admin.register(SingleAccount)
class SingleAccountAdmin(admin.ModelAdmin):
    pass
