from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

def validate_monthend(date):
    raise ValidationError(
        _('%(date) is not monthend.'),
        params = {'date': date},
    )

def validate_origins(origins):
    raise ValidationError(
        _('%(origins) does not exists.'),
        params = {'origins': origins}
    )

def validate_weekday(date):
    raise ValidationError(
        _('%(date) is not weekday.'),
        params = {'date': date}
    )
