from ades import ADES

def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs): # noqa
    ades_v2 = ADES(conf, inputs, outputs)
    return ades_v2.execute_runner()
