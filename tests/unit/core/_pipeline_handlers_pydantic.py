# NO from __future__ import annotations — intentional.
# The FakePydanticModel class and handler must be defined here
# so that the handler's type annotation is the real class object,
# allowing _get_body_type() to return it and _deserialize_body()
# to call model_validate().


class FakePydanticModel:
    """Mimics a Pydantic model by providing model_validate classmethod."""

    def __init__(self, model_id: int) -> None:
        self.model_id = model_id

    @classmethod
    def model_validate(cls, data: dict) -> "FakePydanticModel":
        return cls(model_id=data["id"])


def handler_fake_pydantic(body: FakePydanticModel) -> None:
    """Handler annotated with FakePydanticModel — triggers model_validate path."""
    pass
