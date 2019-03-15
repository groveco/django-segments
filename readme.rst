django-segments allows you slice and dice your user models into SEGMENTS using arbitrary SQL queries,
statis lists of IDs, or even by specifying a model type and a method on that model's .objects manager (e.g.
ORM code).

Assumes your Django user model has an integer primary key called 'id'.

What you do with those segments is up to you. Create a segment, and use the mixin with your user class::

    from django.contrib.auth.models import AbstractUser
    from segments.models import SegmentMixin
    
    class SegmentableUser(AbstractUser, SegmentMixin):
        pass

    ...
    
    u = SegmentableUser()
    s = Segment(definition = "select * from %s" % SegmentableUser._meta.db_table)
    print u.is_member(s)  # "True"

You can use it for targeting marketing offers at certain users, building mailing lists, identifying "good" vs. "bad" customers, and quickly adding all sorts of properties on user records into the django admin without having to write or deploy code.

For instance::

    class Offer(models.Model)
        priority = models.IntegerField()
        discount = models.DecimalField()
        segment = models.ForeignKey(Segment)
    
        class Meta:
            ordering = ('priority', )
    
        @classmethod
        def get_offer_for_user(cls, user)
            for offer in cls.objects.all():
                if offer.segment.has_member(user):
                    return offer


The code is thoroughly documented and tested.

To use, first install (pypi package coming soon)::

    pip install -e git+https://github.com/groveco/django-segments#egg=segments

Then add the following to your settings.py::

    INSTALLED_APPS = (
        ...
        'segments',
    )
    
    # This is the name of the connection Segments will use to evaluate segment SQL
    # Recommended to set this to a readonly DB role. Defaults to 'default'.
    SEGMENTS_EXEC_CONNECTION = 'readonly'
    
You're ready to go!

## Tests

>>> python manage.py test --settings=segments.tests.settings
