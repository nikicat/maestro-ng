# Copyright (C) 2013 SignalFuse, Inc.
#
# Docker container orchestration utility.

from __future__ import print_function

import collections
import json
import sys
import time

from . import exceptions


# Some utility functions for output.
def color(cond):
    """Returns 32 (green) or 31 (red) depending on the validity of the given
    condition."""
    return cond and 32 or 31


def up(cond):
    """Returns 'up' or 'down' depending on the validity of the given
    condition."""
    return cond and 'up' or 'down'


class BaseOrchestrationPlay:
    """Base class for orchestration plays, holds the ordered list containers to
    act on."""

    def __init__(self, containers=[]):
        self._containers = containers

    def run(self):
        raise NotImplementedError

# (argument_name, column_head, header_format, body_format)
DEFAULT_FIELDS = [('order', '  #', '{:>3s}  ', '{:>3d}. '),
                  ('instance', 'INSTANCE', '{:<20s} ',
                   '{escapes[yellow]}{:<20.20s}{escapes[reset]} '),
                  ('service', 'SERVICE', '{:<15s} ', '{:<15.15s} '),
                  ('ship', 'SHIP', '{:<20s} ', '{:<20.20s} '),
                  ('container', 'CONTAINER', '{:<15s}', '{:<15s} ')]

STATUS_FIELD = ('status', 'STATUS', '{:<20s}', '{:<20s}')

escapes = {'yellow': '\033[;1m',
           'reset': '\033[;0m',
           'clear_eol': '\033[K',
           'clear_bol': '\033[1K',
           'color': '\033[{color:d};1m',
           'return': '\r'}


class OutputFormatter:
    """Output formatter for nice, progressive terminal output.

    Manages the output of a progressively updated terminal line, with "in
    progress" labels and a "committed" base label.
    """
    def __init__(self, fields, styled, header):
        self._fields = fields
        self._values = {}
        self._styled = styled
        self._escapes = escapes if styled else collections.defaultdict(str)
        self._header = header
        self._saved = ''

    def header(self):
        if self._header:
            header = ''.join([fmt.format(name)
                             for _, name, fmt, _ in self._fields])
            print('{}{escapes[clear_eol]}'.format(header,
                  escapes=self._escapes))

    def output(self, s):
        print(s, end='')
        sys.stdout.flush()

    def update(self, **kwargs):
        self._values.update(kwargs)

        def values():
            for key, _, _, fmt in self._fields:
                if key + '_color' in kwargs:
                    fmt = '{escapes[color]}{{format}}{escapes[reset]}' \
                        .format(escapes=self._escapes) \
                        .format(format=fmt, color=kwargs[key+'_color'])
                yield fmt.format(self._values.get(key, ''),
                                 escapes=self._escapes)
        row = ''.join(values())
        s = '{}{escapes[clear_eol]}{escapes[return]}' \
            .format(row, escapes=self._escapes)
        if self._styled:
            print(s, end='')
            sys.stdout.flush()
        else:
            self._saved = s

    def end(self):
        print(self._saved)


class FullStatus(BaseOrchestrationPlay):
    """A Maestro orchestration play that displays the status of the given
    services and/or instance containers."""

    def __init__(self, containers=[]):
        BaseOrchestrationPlay.__init__(self, containers)

    def run(self, formatter):
        o = formatter(DEFAULT_FIELDS + [STATUS_FIELD])
        o.header()

        for order, container in enumerate(self._containers, 1):
            o.update(order=order,
                     instance=container.name,
                     service=container.service.name,
                     ship=container.ship.name)

            try:
                o.update(container='checking container...')
                status = container.status()
                o.update(container_color=color(status and
                                               status['State']['Running']),
                         container=container.id[:7] if
                         status and status['State']['Running'] else 'down')

                o.update(status='checking service...')
                running = status and status['State']['Running']
                o.update(status_color=color(running), status=up(running))
                o.end()

                port_output = formatter([('prefix', None, None, '     >> '),
                                         ('port', None, None, '{:>9.9s}'),
                                         ('name', None, None, ':{:s}')])
                for name, port in container.ports.items():
                    port_output.update(port=port['external'][1], name=name)
                    ping = container.ping_port(name)
                    port_output.update(port=port['external'][1],
                                       port_color=color(ping), name=name)
                    port_output.end()
            except Exception:
                o.update(container='host down', container_color=color(False),
                         status='down', status_color=color(False))
                o.end()


class Status(BaseOrchestrationPlay):
    """A less advanced, but faster status display orchestration play that only
    looks at the presence and status of the containers. Status information is
    bulk-polled from each ship's Docker daemon."""

    def __init__(self, containers=[]):
        BaseOrchestrationPlay.__init__(self, containers)

    def run(self, formatter):
        status = {}
        o = formatter([STATUS_FIELD])
        for ship in set([container.ship for container in self._containers]):
            o.update(status='Gathering container information ' +
                            'from {} ({})...'.format(ship.name, ship.ip))
            try:
                status.update(dict((c['Names'][0][1:], c)
                              for c in ship.backend.containers()))
            except:
                pass

        o = formatter(DEFAULT_FIELDS)
        o.header()

        for order, container in enumerate(self._containers, 1):
            o.update(order=order,
                     instance=container.name,
                     service=container.service.name,
                     ship=container.ship.name)

            s = status.get(container.name)
            if s and s['Status'].startswith('Up'):
                cid = s.get('ID', s.get('Id', None))
                o.update(container=cid[:7], container_color=color(True))
            else:
                o.update(container='down', container_color=color(False))
            o.end()


class Start(BaseOrchestrationPlay):
    """A Maestro orchestration play that will execute the start sequence of the
    requested services, starting each container for each instance of the
    services, in the given start order, waiting for each container's
    application to become available before moving to the next one."""

    def __init__(self, containers=[], registries={}, refresh_images=False):
        BaseOrchestrationPlay.__init__(self, containers)
        self._registries = registries
        self._refresh_images = refresh_images

    def run(self, formatter):
        o = formatter(DEFAULT_FIELDS + [STATUS_FIELD])
        o.header()

        for order, container in enumerate(self._containers, 1):
            o.update(order=order,
                     instance=container.name,
                     service=container.service.name,
                     ship=container.ship.name)

            error = None
            try:
                # TODO: None is used to indicate that no action was performed
                # because the container and its application were already
                # running. This makes the following code not very nice and this
                # could be improved.
                result = self._start_container(o, container)
                o.update(status_color=color(result is not False),
                         status='up' if result is None else
                         ('started' if result else 'service did not start!'))
                if result is False:
                    error = [
                        ('Halting start sequence because {} failed to start!'
                            .format(container)),
                        container.ship.backend.logs(container.id)]
                    raise exceptions.OrchestrationException('\n'.join(error))
            except Exception:
                o.update(status_color=color(False),
                         status='failed to start container!')
                raise
            finally:
                o.end()

    def _update_pull_progress(self, progress, last):
        """Update an image pull progress map with latest download progress
        information for one of the image layers, and return the average of the
        download progress of all layers as an indication of the overall
        progress of the pull."""
        try:
            last = json.loads(last)
            progress[last['id']] = last['status'] == 'Download complete' \
                and 100 \
                or (100.0 * last['progressDetail']['current'] /
                    last['progressDetail']['total'])
        except:
            pass

        return reduce(lambda x, y: x+y, progress.values()) / len(progress) \
            if progress else 0

    def _wait_for_status(self, container, cond, retries=10):
        while retries >= 0:
            status = container.status(refresh=True)
            if cond(status):
                return True
            time.sleep(0.5)
            retries -= 1
        return False

    def _login_to_registry(self, o, container):
        """Extract the registry name from the image needed for the container,
        and if authentication data is provided for that registry, login to it
        so a subsequent pull operation can be performed."""
        image = container.service.get_image_details()
        if image['repository'].find('/') <= 0:
            return

        registry, repo_name = image['repository'].split('/', 1)
        if registry not in self._registries:
            return

        o.pending('logging in to {}...'.format(registry))
        try:
            container.ship.backend.login(**self._registries[registry])
        except Exception as e:
            raise exceptions.OrchestrationException(
                'Login to {} failed: {}'.format(registry, e))

    def _start_container(self, o, container):
        """Start the given container.

        If the container and its application are already running, no action is
        performed and the function returns None to indicate that. Otherwise, a
        new container must be created and started. To achieve this, any
        existing container of the same name is first removed. Then, if
        necessary or if requested, the container image is pulled from its
        registry. Finally, the container is created and started, configured as
        necessary. We then wait for the application to start and return True or
        False depending on whether the start was successful."""
        o.update(status='checking service...')
        status = container.status(refresh=True)

        if status and status['State']['Running']:
            o.update(container_color=34,
                     container='{:<15s}'.format(container.id[:7]))
            # We use None as a special marker showing the container and the
            # application were already running.
            return None

        # Otherwise we need to start it.
        if container.id:
            o.update(status='removing old container {}...'.format(
                container.id[:7]))
            container.ship.backend.remove_container(container.id)

        # Check if the image is available, or if we need to pull it down.
        image = container.service.get_image_details()
        if self._refresh_images or \
                not filter(lambda i: container.service.image in i['RepoTags'],
                           container.ship.backend.images(image['repository'])):
            # First, attempt to login if we can/need to.
            self._login_to_registry(o, container)
            o.update(status='pulling image {}...'.format(
                container.service.image))
            progress = {}
            for dlstatus in container.ship.backend.pull(stream=True, **image):
                o.update(status='... {:.1f}%'.format(
                    self._update_pull_progress(progress, dlstatus)))

        # Create and start the container.
        o.update(status='creating container from {}...'.format(
            container.service.image))
        ports = container.ports \
            and map(lambda p: tuple(p['exposed'].split('/')),
                    container.ports.itervalues()) \
            or None
        container.ship.backend.create_container(
            image=container.service.image,
            hostname=container.name,
            name=container.name,
            environment=container.env,
            volumes=container.volumes.values(),
            mem_limit=container.mem_limit,
            cpu_shares=container.cpu_shares,
            ports=ports,
            detach=True,
            command=container.cmd)

        o.update(status='waiting for container creation...')
        if not self._wait_for_status(container, lambda x: x):
            raise exceptions.OrchestrationException(
                'Container status could not be obtained after creation!')
        o.update(container_color=color(True), container=container.id[:7])

        o.update(status='starting container...')
        ports = collections.defaultdict(list) if container.ports else None
        if ports is not None:
            for port in container.ports.values():
                ports[port['exposed']].append(
                    (port['external'][0], port['external'][1].split('/')[0]))
        container.ship.backend.start(container.id,
                                     binds=container.volumes,
                                     port_bindings=ports,
                                     privileged=container.privileged)

        # Waiting one second and checking container state again to make sure
        # initialization didn't fail.
        o.update(status='waiting for container initialization...')
        if not self._wait_for_status(container,
                                     lambda x: x and x['State']['Running']):
            raise exceptions.OrchestrationException(
                'Container status could not be obtained after start!')

        # Wait up for the container's application to come online.
        o.update(status='waiting for service...')
        return container.check_for_state('running') is not False


class Stop(BaseOrchestrationPlay):
    """A Maestro orchestration play that will stop and remove the containers of
    the requested services. The list of containers should be provided reversed
    so that dependent services are stopped first."""

    def __init__(self, containers=[]):
        BaseOrchestrationPlay.__init__(self, containers)

    def run(self, formatter):
        o = formatter(DEFAULT_FIELDS + [STATUS_FIELD])
        o.header()

        for order, container in enumerate(self._containers):
            o.update(order=len(self._containers) - order,
                     instance=container.name,
                     service=container.service.name,
                     ship=container.ship.name)

            o.update(status='checking container...')
            try:
                status = container.status(refresh=True)
                if not status or not status['State']['Running']:
                    o.update(container='n/a', status='already down')
                    o.end()
                    continue
            except:
                o.update(container_color=color(False),
                         container='ship down', status='down')
                o.end()
                continue

            o.update(container=container.id[:7])

            try:
                o.update(status='stopping service...')
                container.ship.backend.stop(container.id,
                                            timeout=container.stop_timeout)
                container.check_for_state('stopped')
                o.update(status_color=color(True), status='stopped')
            except:
                o.update(status_color=color(False), status='fail!')

            o.end()
